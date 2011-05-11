package com.sensei.search.nodes;


import org.apache.log4j.Logger;

import com.linkedin.norbert.NorbertException;
import com.linkedin.norbert.javacompat.cluster.ClusterClient;
import com.linkedin.norbert.javacompat.network.PartitionedNetworkClient;
import com.linkedin.norbert.javacompat.network.PartitionedLoadBalancerFactory;
import com.sensei.search.req.AbstractSenseiRequest;
import com.sensei.search.req.AbstractSenseiResult;
import com.sensei.search.svc.api.SenseiException;

/**
 * @author "Xiaoyang Gu<xgu@linkedin.com>"
 * 
 * @param <REQUEST>
 * @param <RESULT>
 */
public abstract class AbstractSenseiBroker<REQUEST extends AbstractSenseiRequest, RESULT extends AbstractSenseiResult>
{
  private final static Logger logger = Logger.getLogger(AbstractSenseiBroker.class);
  protected final PartitionedNetworkClient<Integer> _networkClient;
  protected final ClusterClient _clusterClient;
  protected final PartitionedLoadBalancerFactory<Integer> _loadBalancerFactory;


  /**
   * @param networkClient
   * @param clusterClient
   * @param routerFactory
   * @throws NorbertException
   */
  public AbstractSenseiBroker(PartitionedNetworkClient<Integer> networkClient, ClusterClient clusterClient, PartitionedLoadBalancerFactory<Integer> loadBalancerFactory)
      throws NorbertException
  {
	  _loadBalancerFactory = loadBalancerFactory;
    _networkClient = networkClient;
    _clusterClient = clusterClient;
    // register the request-response messages
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
  public RESULT browse(final REQUEST req) throws SenseiException {
    try
    {
    	return doBrowse(_networkClient, req);

    } catch (Exception e)
    {
      throw new SenseiException(e.getMessage(), e);
    }
  }

  protected abstract RESULT doBrowse(PartitionedNetworkClient<Integer> networkClient, REQUEST req) throws Exception;

  public void shutdown()
  {
    logger.info("shutting down broker...");
  }

  public abstract void setTimeoutMillis(long timeoutMillis);

  public abstract long getTimeoutMillis();

}
