package com.sensei.search.nodes;

import it.unimi.dsi.fastutil.ints.IntSet;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.google.protobuf.Message;
import com.linkedin.norbert.NorbertException;
import com.linkedin.norbert.javacompat.cluster.ClusterClient;
import com.linkedin.norbert.javacompat.network.PartitionedNetworkClient;
import com.sensei.search.cluster.routing.RoutingInfo;
import com.sensei.search.cluster.routing.SenseiLoadBalancer;
import com.sensei.search.cluster.routing.SenseiLoadBalancerFactory;
import com.sensei.search.req.AbstractSenseiRequest;
import com.sensei.search.req.AbstractSenseiResult;
import com.sensei.search.svc.api.SenseiException;

/**
 * @author "Xiaoyang Gu<xgu@linkedin.com>"
 *
 * @param <REQUEST>
 * @param <RESULT>
 * @param <REQMSG>
 * @param <RESMSG>
 */
public abstract class AbstractConsistentHashBroker<REQUEST extends AbstractSenseiRequest, RESULT extends AbstractSenseiResult, REQMSG extends Message, RESMSG extends Message>
    extends AbstractSenseiBroker<REQUEST, RESULT, REQMSG, RESMSG>
{
  private final static Logger logger = Logger.getLogger(AbstractConsistentHashBroker.class);
  protected long _timeout = 8000;
  protected SenseiLoadBalancer _loadBalancer;
  protected final SenseiLoadBalancerFactory _loadBalancerFactory;

  /**
   * @param networkClient
   * @param clusterClient
   * @param defaultrequest
   *          a default instance of request message object for protobuf
   *          registration
   * @param defaultresult
   *          a default instance of result message object for protobuf
   *          registration
   * @param routerFactory
   * @param scatterGatherHandler
   * @throws NorbertException
   */
  public AbstractConsistentHashBroker(PartitionedNetworkClient<Integer> networkClient, ClusterClient clusterClient, REQMSG defaultrequest, RESMSG defaultresult, SenseiLoadBalancerFactory loadBalancerFactory)
      throws NorbertException
  {
    super(networkClient, clusterClient, defaultrequest, defaultresult,null);
    _loadBalancerFactory = loadBalancerFactory;
  }

  public abstract REQMSG requestToMessage(REQUEST request);

  public abstract RESULT messageToResult(RESMSG message);

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
    if (_partitions == null)
      throw new SenseiException("Browse called before cluster is connected!");
    try
    {
      return doBrowse(_networkClient, req, _partitions);
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
  public abstract String getRouteParam(REQUEST req);

  protected RESULT doBrowse(PartitionedNetworkClient<Integer> networkClient, REQUEST req, IntSet partitions)
  {
    long time = System.currentTimeMillis();
    RoutingInfo searchNodes = _loadBalancer.route(getRouteParam(req));
    @SuppressWarnings("unchecked")
    Future<RESMSG>[] responseFutures = new Future[searchNodes.partitions.length];
    List<RESULT> resultlist = new ArrayList<RESULT>(searchNodes.partitions.length);
    for(int ni = 0; ni < searchNodes.partitions.length; ni++)
    {
      HashSet<Integer> pset = new HashSet<Integer>();
      pset.add(searchNodes.partitions[ni]);
      req.setPartitions(pset);
      REQMSG msg = requestToMessage(req);
      if (logger.isDebugEnabled())
      {
        logger.info("DEBUG: broker sending req part: " + searchNodes.partitions[ni] + " on node: " + searchNodes.nodelist[ni].get(searchNodes.nodegroup[ni]));
      }
      responseFutures[ni] = (Future<RESMSG>) _networkClient.sendMessageToNode(msg, searchNodes.nodelist[ni].get(searchNodes.nodegroup[ni]));
    }
    for(int ni = 0; ni < searchNodes.partitions.length; ni++)
    { 
      RESMSG resp;
      try
      {
        resp = responseFutures[ni].get(_timeout,TimeUnit.MILLISECONDS);
        RESULT res = messageToResult(resp);
        resultlist.add(res);
        if (logger.isDebugEnabled())
        {
          logger.info("DEBUG: broker receiving res part: " + searchNodes.partitions[ni] + " on node: " + searchNodes.nodelist[ni].get(searchNodes.nodegroup[ni])
              + " node time: " + res.getTime() +"ms remote time: " + (System.currentTimeMillis() - time) + "ms");
        }
      } catch (Exception e)
      {
        logger.error("DEBUG: broker receiving res part: " + searchNodes.partitions[ni] + " on node: " + searchNodes.nodelist[ni].get(searchNodes.nodegroup[ni])
            + e +" remote time: " + (System.currentTimeMillis() - time) + "ms");
      }
    }
    if (resultlist.size() == 0)
    {
      logger.error("no result received at all return empty result");
      return getEmptyResultInstance();
    }
    RESULT result = mergeResults(req, resultlist);
    logger.info("remote search took " + (System.currentTimeMillis() - time) + "ms");
    return result;
  }

  public void shutdown()
  {
    logger.info("shutting down broker...");
  }

  public abstract void setTimeoutMillis(long timeoutMillis);

  public abstract long getTimeoutMillis();

}
