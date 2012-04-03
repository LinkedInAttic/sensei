package com.senseidb.search.node;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.linkedin.norbert.NorbertException;
import com.linkedin.norbert.javacompat.cluster.Node;
import com.linkedin.norbert.javacompat.network.PartitionedNetworkClient;
import com.linkedin.norbert.network.Serializer;
import com.senseidb.cluster.routing.RoutingInfo;
import com.senseidb.cluster.routing.SenseiLoadBalancer;
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
  protected volatile SenseiLoadBalancer _loadBalancer;
  protected final int _pollInterval;
  protected final int _minResponses;
  protected final int _maxTotalWait;
  
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
  public AbstractConsistentHashBroker(PartitionedNetworkClient<Integer> networkClient,
                                      Serializer<REQUEST, RESULT> serializer,
                                      int pollInterval,
                                      int minResponses,
                                      int maxTotalWait)
      throws NorbertException
  {
    super(networkClient);
    _loadBalancer = null;
    _serializer = serializer;
    _pollInterval = pollInterval;
    _minResponses = minResponses;
    _maxTotalWait = maxTotalWait;
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
    int[] partArray = null;
    RoutingInfo searchNodeInfo = null;

    if (_loadBalancer != null)
    {
      searchNodeInfo = _loadBalancer.route(getRouteParam(req));
      if (searchNodeInfo != null)
      {
    	partArray = searchNodeInfo.partitions;
      }
    }
    
    if (partArray == null)
    {
      logger.info("No search nodes to handle request...");
      EmptyMeter.mark();
      return getEmptyResultInstance();
    }
    
    final int[] parts = partArray;
    final RoutingInfo searchNodes = searchNodeInfo;

    final List<RESULT> resultlist = new ArrayList<RESULT>(parts.length);
    final Map<Integer, Set<Integer>> partsMap = new HashMap<Integer, Set<Integer>>();
    final Map<Integer, Node> nodeMap = new HashMap<Integer, Node>();
    final Map<Integer, Future<RESULT>> futureMap = new HashMap<Integer, Future<RESULT>>();
    
    try
    {
      ScatterTimer.time(new Callable<Object>() {
          @Override
          public Object call() throws Exception {
            for(int ni = 0; ni < parts.length; ni++)
            {
              Node node = searchNodes.nodelist[ni].get(searchNodes.nodegroup[ni]);
              Set<Integer> pset = partsMap.get(node.getId());
              if (pset == null)
              {
                pset = new HashSet<Integer>();
                partsMap.put(node.getId(), pset);
              }
              pset.add(parts[ni]);
              nodeMap.put(node.getId(), node);
            }
			    
            for (Map.Entry<Integer, Node> entry : nodeMap.entrySet())
            {
              req.setPartitions(partsMap.get(entry.getKey()));
              req.saveState();
              REQUEST thisRequest = customizeRequest(req);
              if (logger.isDebugEnabled()){
                logger.debug("broker sending req part: " + partsMap.get(entry.getKey()) + " on node: " + entry.getValue());
              }
              futureMap.put(entry.getKey(), (Future<RESULT>)_networkClient.sendRequestToNode(thisRequest, entry.getValue(), _serializer));
              req.restoreState();
            }

            int totalTime = 0;
            int interval = _pollInterval;
            int numResults = 0;
            int totalTasks = futureMap.size();
            int minRespExpected = (_minResponses < totalTasks) ? _minResponses : totalTasks;
            while (numResults < minRespExpected ||
                   (numResults < totalTasks && totalTime < _maxTotalWait))
            {
              long startTime = System.currentTimeMillis();
              Thread.sleep(interval);  // Sleep for a small interval. May wake up much later.
              totalTime += (System.currentTimeMillis() - startTime);
              if (totalTime > _timeout)
              {
                logger.error("Hit hard timeout limit on broker.");
                break;
              }
              Iterator itr = futureMap.entrySet().iterator();
              while (itr.hasNext())
              {
                Map.Entry<Integer, Future<RESULT>> entry = (Map.Entry<Integer, Future<RESULT>>) itr.next();
                Future<RESULT> futureRes = entry.getValue();
                if (futureRes.isDone())
                {
                  resultlist.add((RESULT) futureRes.get());
                  itr.remove();
                  numResults++;
                }
              }
            }

            logger.info("totalTime = " + totalTime + ", resultlist.size = " + resultlist.size());
            return null;
          }
    	
        });
    }
    catch(Exception e){
      logger.error(e.getMessage(),e);
      ErrorMeter.mark();
    }
    
   
    if (resultlist.size() == 0)
    {
      logger.error("no result received at all return empty result");
      EmptyMeter.mark();
      return getEmptyResultInstance();
    }
    
    RESULT result;
    try{
      result = GatherTimer.time(new Callable<RESULT>(){

		@Override
		public RESULT call() throws Exception {
			return mergeResults(req, resultlist);
		}
    	
      });
    }
    catch(Exception e){
    	logger.error(e.getMessage(),e);
    	result = getEmptyResultInstance();
    	EmptyMeter.mark();
    	ErrorMeter.mark();
    }
    
    if (logger.isDebugEnabled()){
      logger.debug("remote search took " + (System.currentTimeMillis() - time) + "ms");
    }
    return result;
  }

  public void shutdown()
  {
    logger.info("shutting down broker...");
  }

  public abstract void setTimeoutMillis(long timeoutMillis);

  public abstract long getTimeoutMillis();

}
