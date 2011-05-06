package com.sensei.search.nodes;

import it.unimi.dsi.fastutil.ints.IntSet;

import org.apache.log4j.Logger;

import com.google.protobuf.Message;
import com.linkedin.norbert.NorbertException;
import com.linkedin.norbert.javacompat.cluster.ClusterClient;
import com.linkedin.norbert.javacompat.cluster.ClusterListener;
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
 * @param <REQMSG>
 * @param <RESMSG>
 */
public abstract class AbstractSenseiBroker<REQUEST extends AbstractSenseiRequest, RESULT extends AbstractSenseiResult, REQMSG extends Message, RESMSG extends Message>
    implements ClusterListener
{
  private final static Logger logger = Logger.getLogger(AbstractSenseiBroker.class);
  protected final PartitionedNetworkClient<Integer> _networkClient;
  protected final ClusterClient _clusterClient;
  protected final PartitionedLoadBalancerFactory<Integer> _routerFactory;
  protected volatile IntSet _partitions = null;
  

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
  public AbstractSenseiBroker(PartitionedNetworkClient<Integer> networkClient, ClusterClient clusterClient, 
		  	REQMSG defaultrequest, RESMSG defaultresult,PartitionedLoadBalancerFactory<Integer> routerFactory)
      throws NorbertException
  {
	_routerFactory = routerFactory;
    _networkClient = networkClient;
    _clusterClient = clusterClient;
    // register the request-response messages
    _networkClient.registerRequest(defaultrequest, defaultresult);
    clusterClient.addListener(this);
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
  public RESULT browse(final REQUEST req) throws SenseiException
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

  protected abstract RESULT doBrowse(PartitionedNetworkClient<Integer> networkClient, REQUEST req, IntSet partitions) throws Exception;

  public void shutdown()
  {
    logger.info("shutting down broker...");
  }

  public abstract void setTimeoutMillis(long timeoutMillis);

  public abstract long getTimeoutMillis();

}
