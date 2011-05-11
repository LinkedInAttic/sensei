package com.sensei.search.nodes;

import java.util.*;
import com.linkedin.norbert.network.Serializer;
import com.linkedin.norbert.network.common.TimeoutIterator;
import com.sensei.search.req.SenseiRequest;
import org.apache.log4j.Logger;

import com.linkedin.norbert.NorbertException;
import com.linkedin.norbert.javacompat.cluster.Node;
import com.linkedin.norbert.javacompat.cluster.ClusterClient;
import com.linkedin.norbert.javacompat.network.RequestBuilder;
import com.linkedin.norbert.javacompat.network.PartitionedLoadBalancerFactory;
import com.linkedin.norbert.javacompat.network.PartitionedNetworkClient;
import com.sensei.search.req.AbstractSenseiRequest;
import com.sensei.search.req.AbstractSenseiResult;
import com.sensei.search.svc.api.SenseiException;

/**
 * @author "Xiaoyang Gu<xgu@linkedin.com>"
 *
 * @param <REQUEST>
 * @param <RESULT>
 */
public abstract class AbstractConsistentHashBroker<REQUEST extends AbstractSenseiRequest<REQUEST>, RESULT extends AbstractSenseiResult>
    extends AbstractSenseiBroker<REQUEST, RESULT>
{
  private final static Logger logger = Logger.getLogger(AbstractConsistentHashBroker.class);
  protected long _timeout = 8000;
  protected PartitionedLoadBalancerFactory<Integer> _loadBalancer;
  private final Serializer<REQUEST,RESULT> serializer;

  /**
   * @param networkClient
   * @param clusterClient
   * @param loadBalancerFactory
   * @throws NorbertException
   */
  public AbstractConsistentHashBroker(PartitionedNetworkClient<Integer> networkClient,
                                      ClusterClient clusterClient,
                                      PartitionedLoadBalancerFactory<Integer> loadBalancerFactory,
                                      Serializer<REQUEST, RESULT> serializer)
      throws NorbertException
  {
    super(networkClient, clusterClient, loadBalancerFactory);
    this.serializer = serializer;
  }

  /**
   * @return an empty result instance. Used when the request cannot be properly
   *         processed or when the true result is empty.
   */
  public abstract RESULT getEmptyResultInstance();

  /**
   * The method that provides the search service.
   * 
   * @param req
   * @return
   * @throws SenseiException
   */
  public RESULT browse(REQUEST req) throws SenseiException
  {
    try
    {
      return doBrowse(_networkClient, req);
    } catch (Exception e)
    {
      throw new SenseiException(e.getMessage(), e);
    }
  }

  /**
   * Merge results on the client/broker side. It likely works differently from
   * the one in the search node.
   * 
   * @param request
   *          the original request object
   * @param resultList
   *          the list of results from all the requested partitions.
   * @return one single result instance that is merged from the result list.
   */
  public abstract RESULT mergeResults(REQUEST request, List<RESULT> resultList);

  protected RESULT doBrowse(PartitionedNetworkClient<Integer> networkClient, final REQUEST req)
  {
    long time = System.currentTimeMillis();

    // TODO: Fix this
    Iterator<RESULT> iterator = scala.collection.JavaConversions.asJavaIterator(
    new TimeoutIterator<RESULT>(networkClient.sendRequestToOneReplica(new RequestBuilder<Integer, REQUEST>() {
      @Override
      public REQUEST apply(Node node, Set<Integer> partitions) {
        return req.setPartitions(partitions);
      }
    }, serializer), _timeout));

    List<RESULT> results = toList(iterator);

    if(results.isEmpty())
    {
      logger.error("no result received at all return empty result");
      return getEmptyResultInstance();
    }

    RESULT result = mergeResults(req, results);
    logger.info("remote search took " + (System.currentTimeMillis() - time) + "ms");
    return result;
  }

  public void shutdown()
  {
    logger.info("shutting down broker...");
  }

  public abstract void setTimeoutMillis(long timeoutMillis);

  public abstract long getTimeoutMillis();

  private static <A> List<A> toList(Iterator<A> iter) {
    ArrayList<A> result = new ArrayList<A>();
    while(iter.hasNext())
      result.add(iter.next());
    return result;
  }
}
