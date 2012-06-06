package com.senseidb.search.node;

import it.unimi.dsi.fastutil.ints.IntSet;

import org.apache.log4j.Logger;

import com.google.protobuf.Message;
import com.linkedin.norbert.NorbertException;
import com.linkedin.norbert.javacompat.cluster.ClusterListener;
import com.linkedin.norbert.javacompat.network.PartitionedNetworkClient;
import com.senseidb.search.req.AbstractSenseiRequest;
import com.senseidb.search.req.AbstractSenseiResult;
import com.senseidb.svc.api.SenseiException;

/**
 * @author "Xiaoyang Gu<xgu@linkedin.com>"
 * 
 * @param <REQUEST>
 * @param <RESULT>
 */
public abstract class AbstractSenseiBroker<REQUEST extends AbstractSenseiRequest, RESULT extends AbstractSenseiResult>
    implements ClusterListener, Broker<REQUEST, RESULT>
{
  private final static Logger logger = Logger.getLogger(AbstractSenseiBroker.class);
  protected final PartitionedNetworkClient<String> _networkClient;
  protected volatile IntSet _partitions = null;
  

  /**
   * @param networkClient
   * @param clusterClient
   * @param routerFactory
   * @param scatterGatherHandler
   * @throws NorbertException
   */
  public AbstractSenseiBroker(PartitionedNetworkClient<String> networkClient)
      throws NorbertException
  {
    _networkClient = networkClient;
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

  protected abstract RESULT doBrowse(PartitionedNetworkClient<String> networkClient, REQUEST req, IntSet partitions) throws Exception;

  public void shutdown()
  {
    logger.info("shutting down broker...");
  }

  public abstract void setTimeoutMillis(long timeoutMillis);

  public abstract long getTimeoutMillis();

}
