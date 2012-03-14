package com.senseidb.search.node;

import com.linkedin.norbert.javacompat.network.RequestBuilder;
import com.linkedin.norbert.network.ResponseIterator;
import com.linkedin.norbert.network.common.ExceptionIterator;
import com.linkedin.norbert.network.common.PartialIterator;
import com.linkedin.norbert.network.common.TimeoutIterator;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.linkedin.norbert.NorbertException;
import com.linkedin.norbert.javacompat.cluster.Node;
import com.linkedin.norbert.javacompat.network.PartitionedNetworkClient;
import com.linkedin.norbert.network.Serializer;
import com.senseidb.cluster.routing.RoutingInfo;
import com.senseidb.metrics.MetricsConstants;
import com.senseidb.search.req.AbstractSenseiRequest;
import com.senseidb.search.req.AbstractSenseiResult;
import com.senseidb.svc.api.SenseiException;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.Timer;

/**
 * @author "Xiaoyang Gu<xgu@linkedin.com>"
 *
 * @param <REQUEST>
 * @param <RESULT>
 */
public abstract class AbstractConsistentHashBroker<REQUEST extends AbstractSenseiRequest, RESULT extends AbstractSenseiResult>
    extends AbstractSenseiBroker<REQUEST, RESULT>
{
  private final static Logger logger = Logger.getLogger(AbstractConsistentHashBroker.class);

  protected long _timeout = 8000;
  protected final Serializer<REQUEST, RESULT> _serializer;

  private static Timer ScatterTimer = null;
  private static Timer GatherTimer = null;
  private static Timer TotalTimer = null;
  private static Meter ErrorMeter = null;
  private static Meter EmptyMeter = null;
  
  static{
	  // register metrics monitoring for timers
	  try{
	    MetricName scatterMetricName = new MetricName(MetricsConstants.Domain,"timer","scatter-time","broker");
	    ScatterTimer = Metrics.newTimer(scatterMetricName, TimeUnit.MILLISECONDS,TimeUnit.SECONDS);
	    
	    MetricName gatherMetricName = new MetricName(MetricsConstants.Domain,"timer","gather-time","broker");
	    GatherTimer = Metrics.newTimer(gatherMetricName, TimeUnit.MILLISECONDS,TimeUnit.SECONDS);

	    MetricName totalMetricName = new MetricName(MetricsConstants.Domain,"timer","total-time","broker");
	    TotalTimer = Metrics.newTimer(totalMetricName, TimeUnit.MILLISECONDS,TimeUnit.SECONDS);
	    
	    MetricName errorMetricName = new MetricName(MetricsConstants.Domain,"meter","error-meter","broker");
	    ErrorMeter = Metrics.newMeter(errorMetricName, "errors",TimeUnit.SECONDS);
	    
	    MetricName emptyMetricName = new MetricName(MetricsConstants.Domain,"meter","empty-meter","broker");
	    EmptyMeter = Metrics.newMeter(emptyMetricName, "null-hits", TimeUnit.SECONDS);
	  }
	  catch(Exception e){
		logger.error(e.getMessage(),e);
	  }
  }
  
  /**
   * @param networkClient
   * @param clusterClient
   * @param routerFactory
   * @param serializer
   *          The serializer used to serialize/deserialize request/response pairs
   * @param scatterGatherHandler
   * @throws NorbertException
   */
  public AbstractConsistentHashBroker(PartitionedNetworkClient<Integer> networkClient, Serializer<REQUEST, RESULT> serializer)
      throws NorbertException
  {
    super(networkClient);
    _serializer = serializer;
  }

	public <T> T customizeRequest(REQUEST request)
	{
		return null;
	}
	
	public <T> T restoreRequest(REQUEST request,T state){
		return state;
	}

  protected IntSet getPartitions(Set<Node> nodes)
  {
	    IntSet partitionSet = new IntOpenHashSet();
	    for (Node n : nodes)
	    {
	      partitionSet.addAll(n.getPartitionIds());
	    }
	    return partitionSet;
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
    if (_partitions == null){
      ErrorMeter.mark();
      throw new SenseiException("Browse called before cluster is connected!");
    }
    try
    {
      return TotalTimer.time(new Callable<RESULT>(){
    	@Override
  		public RESULT call() throws Exception {
          return doBrowse(_networkClient, req, _partitions); 	  
    	}
      });
    } 
    catch (Exception e)
    {
      ErrorMeter.mark();
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
  
  

  protected RESULT doBrowse(PartitionedNetworkClient<Integer> networkClient, final REQUEST req, IntSet partitions)
  {
    final long time = System.currentTimeMillis();

    final List<RESULT> resultList = new ArrayList<RESULT>();
    try {
      resultList.addAll(ScatterTimer.time(new Callable<List<RESULT>>() {
        @Override
        public List<RESULT> call() throws Exception {
          return doCall(req);
        }
      }));
    } catch (Exception e) {
      ErrorMeter.mark();
      logger.error("Error running scatter/gather", e);
      return getEmptyResultInstance();
    }

    if (resultList.size() == 0)
    {
      logger.error("no result received at all return empty result");
      EmptyMeter.mark();
    }

    RESULT result = null;
    try {
      result = GatherTimer.time(new Callable<RESULT>() {
        @Override
        public RESULT call() throws Exception {
          return mergeResults(req, resultList);
        }
      });
    } catch (Exception e) {
      result = getEmptyResultInstance();
      logger.error("Error gathering the results", e);
      ErrorMeter.mark();
    }

    if (logger.isDebugEnabled()){
      logger.debug("remote search took " + (System.currentTimeMillis() - time) + "ms");
    }

    return result;
  }

  protected List<RESULT> doCall(final REQUEST req) throws ExecutionException {
    List<RESULT> resultList = new ArrayList<RESULT>();
    ResponseIterator<RESULT> responseIterator =
        _networkClient.sendRequestToOneReplica(new RequestBuilder<Integer, REQUEST>() {
          private int count = 0;
          @Override
          public REQUEST apply(Node node, Set<Integer> nodePartitions) {
            synchronized (req) {
              req.setPartitions(nodePartitions);

              if(count != 0)
                req.restoreState();

              req.saveState();
              REQUEST customizedRequest = customizeRequest(req);

              count++;
              return customizedRequest;
            }
          }
        }, _serializer);

    TimeoutIterator<RESULT> timeoutIterator = new TimeoutIterator<RESULT>(responseIterator, _timeout);
//          PartialIterator<RESULT> partialIterator = new PartialIterator<RESULT>(new ExceptionIterator<RESULT>(timeoutIterator));

    while(responseIterator.hasNext()) {
      resultList.add(responseIterator.next());
    }

    logger.debug(String.format("There are %d responses", resultList.size()));

    return resultList;
  }

  public void shutdown()
  {
    logger.info("shutting down broker...");
  }

  public abstract void setTimeoutMillis(long timeoutMillis);

  public abstract long getTimeoutMillis();

}
