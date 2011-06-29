package com.sensei.search.nodes;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.management.ObjectName;

import org.apache.log4j.Logger;

import com.google.protobuf.Message;
import com.linkedin.norbert.NorbertException;
import com.linkedin.norbert.javacompat.cluster.Node;
import com.linkedin.norbert.javacompat.network.PartitionedNetworkClient;
import com.sensei.search.cluster.routing.RoutingInfo;
import com.sensei.search.cluster.routing.SenseiLoadBalancer;
import com.sensei.search.jmx.JmxUtil;
import com.sensei.search.jmx.JmxUtil.Timer;
import com.sensei.search.req.AbstractSenseiRequest;
import com.sensei.search.req.AbstractSenseiResult;
import com.sensei.search.svc.api.SenseiException;
import com.yammer.metrics.core.CounterMetric;
import com.yammer.metrics.core.TimerMetric;
import com.yammer.metrics.reporting.JmxReporter.Counter;

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
  protected volatile SenseiLoadBalancer _loadBalancer;
  
  private final static TimerMetric ScatterTimer = new TimerMetric(TimeUnit.MILLISECONDS,TimeUnit.SECONDS);
  private final static TimerMetric GatherTimer = new TimerMetric(TimeUnit.MILLISECONDS,TimeUnit.SECONDS);
  private final static TimerMetric TotalTimer = new TimerMetric(TimeUnit.MILLISECONDS,TimeUnit.SECONDS);
  private final static CounterMetric ErrorCounter = new CounterMetric();
  private final static CounterMetric EmptyCounter = new CounterMetric();
  
  static{
	  // register jmx monitoring for timers
	  try{
	    ObjectName scatterMBeanName = new ObjectName(JmxUtil.Domain+".broker","name","scatter-time");
	    JmxUtil.registerMBean(ScatterTimer, scatterMBeanName);
	    
	    ObjectName gatherMBeanName = new ObjectName(JmxUtil.Domain+".broker","name","gather-time");
	    JmxUtil.registerMBean(GatherTimer, gatherMBeanName);

	    ObjectName totalMBeanName = new ObjectName(JmxUtil.Domain+".broker","name","total-time");
	    JmxUtil.registerMBean(TotalTimer, totalMBeanName); 
	    
	    ObjectName errorCounterMBeanName = new ObjectName(JmxUtil.Domain+".broker","name","error-count");
	    JmxUtil.registerMBean(ErrorCounter, errorCounterMBeanName); 
	    
	    ObjectName emptyCounterMBeanName = new ObjectName(JmxUtil.Domain+".broker","name","empty-count");
	    JmxUtil.registerMBean(EmptyCounter, emptyCounterMBeanName); 
	  }
	  catch(Exception e){
		logger.error(e.getMessage(),e);
	  }
  }
  
  

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
  public AbstractConsistentHashBroker(PartitionedNetworkClient<Integer> networkClient, REQMSG defaultrequest, RESMSG defaultresult)
      throws NorbertException
  {
    super(networkClient, defaultrequest, defaultresult);
    _loadBalancer = null;
  }

  public abstract REQMSG requestToMessage(REQUEST request);

  public abstract RESULT messageToResult(RESMSG message);
  
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
      ErrorCounter.inc();
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
      ErrorCounter.inc();
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
      EmptyCounter.inc();
      return getEmptyResultInstance();
    }
    
    final int[] parts = partArray;
    final RoutingInfo searchNodes = searchNodeInfo;

    final List<RESULT> resultlist = new ArrayList<RESULT>(parts.length);
    final Map<Integer, Set<Integer>> partsMap = new HashMap<Integer, Set<Integer>>();
    final Map<Integer, Node> nodeMap = new HashMap<Integer, Node>();
    final Map<Integer, Future<RESMSG>> futureMap = new HashMap<Integer, Future<RESMSG>>();
    
    try{
      ScatterTimer.time(new Callable<Object>(){

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
			      REQMSG msg = requestToMessage(req);
			      if (logger.isDebugEnabled())
			      {
			        logger.debug("broker sending req part: " + partsMap.get(entry.getKey()) + " on node: " + entry.getValue());
			      }
			      futureMap.put(entry.getKey(), (Future<RESMSG>)_networkClient.sendMessageToNode(msg, entry.getValue()));
			    }
			    for(Map.Entry<Integer, Future<RESMSG>> entry : futureMap.entrySet())
			    { 
			      RESMSG resp;
			      try
			      {
			        resp = entry.getValue().get(_timeout,TimeUnit.MILLISECONDS);
			        RESULT res = messageToResult(resp);
			        resultlist.add(res);
			        if (logger.isDebugEnabled())
			        {
			          logger.debug("broker receiving res part: " + partsMap.get(entry.getKey()) + " on node: " + nodeMap.get(entry.getKey())
			              + " node time: " + res.getTime() +"ms remote time: " + (System.currentTimeMillis() - time) + "ms");
			        }
			      } catch (Exception e)
			      {
			    	ErrorCounter.inc();
			        logger.error("broker receiving res part: " + partsMap.get(entry.getKey()) + " on node: " + nodeMap.get(entry.getKey())
			            + e +" remote time: " + (System.currentTimeMillis() - time) + "ms");
			      }
			    }
			    return null;
		}
    	
      });
    }
    catch(Exception e){
      logger.error(e.getMessage(),e);
      ErrorCounter.inc();
    }
    
   
    if (resultlist.size() == 0)
    {
      logger.error("no result received at all return empty result");
      EmptyCounter.inc();
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
    	EmptyCounter.inc();
    	ErrorCounter.inc();
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
