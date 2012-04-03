package com.senseidb.search.node;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.linkedin.norbert.NorbertException;
import com.linkedin.norbert.javacompat.cluster.ClusterClient;
import com.linkedin.norbert.javacompat.cluster.Node;
import com.linkedin.norbert.javacompat.network.PartitionedNetworkClient;
import com.senseidb.cluster.routing.RoutingInfo;
import com.senseidb.cluster.routing.SenseiLoadBalancerFactory;
import com.senseidb.search.req.SenseiRequest;
import com.senseidb.search.req.SenseiSystemInfo;
import com.senseidb.svc.impl.SysSenseiCoreServiceImpl;

public class SenseiSysBroker extends AbstractConsistentHashBroker<SenseiRequest, SenseiSystemInfo>
{
  private final static Logger logger = Logger.getLogger(SenseiSysBroker.class);
  private final static long TIMEOUT_MILLIS = 8000L;
  private long _timeoutMillis = TIMEOUT_MILLIS;
  private final Comparator<String> _versionComparator;
  private final SenseiLoadBalancerFactory _loadBalancerFactory;

  protected Set<Node> _nodes = Collections.EMPTY_SET;

  public SenseiSysBroker(PartitionedNetworkClient<Integer> networkClient,
                         ClusterClient clusterClient,
                         SenseiLoadBalancerFactory loadBalancerFactory,
                         Comparator<String> versionComparator,
                         int pollInterval,
                         int minResponses,
                         int maxTotalWait)
    throws NorbertException
  {
    super(networkClient, SysSenseiCoreServiceImpl.PROTO_SERIALIZER, pollInterval, minResponses, maxTotalWait);
    _versionComparator = versionComparator;
    _loadBalancerFactory = loadBalancerFactory;
    clusterClient.addListener(this);
    logger.info("created broker instance " + networkClient + " " + clusterClient + " " + loadBalancerFactory);
  }

  @Override
  public SenseiSystemInfo mergeResults(SenseiRequest request, List<SenseiSystemInfo> resultList)
  {
    SenseiSystemInfo result = new SenseiSystemInfo();
    if (resultList == null)
      return result;

    for (SenseiSystemInfo res : resultList)
    {
      result.setNumDocs(result.getNumDocs()+res.getNumDocs());
      result.setSchema(res.getSchema());
      if (result.getLastModified() < res.getLastModified())
        result.setLastModified(res.getLastModified());
      if (result.getVersion() == null || _versionComparator.compare(result.getVersion(), res.getVersion()) < 0)
        result.setVersion(res.getVersion());
      if (res.getFacetInfos() != null)
        result.setFacetInfos(res.getFacetInfos());
      if (res.getClusterInfo() != null) {
        if (result.getClusterInfo() != null)
          result.getClusterInfo().addAll(res.getClusterInfo());
        else
          result.setClusterInfo(res.getClusterInfo());
      }
    }

    return result;
  }
  
  @Override
  protected SenseiSystemInfo doBrowse(PartitionedNetworkClient<Integer> networkClient, SenseiRequest req, IntSet partitions)
  {
    long time = System.currentTimeMillis();
    int[] parts = null;
    RoutingInfo searchNodes = null;

    if (_loadBalancer != null)
    {
      searchNodes = _loadBalancer.route(getRouteParam(req));
      if (searchNodes != null)
      {
        parts = searchNodes.partitions;
      }
    }
    
    if (parts == null)
    {
      logger.info("No search nodes to handle request...");
      return getEmptyResultInstance();
    }
    
    List<SenseiSystemInfo> resultlist = new ArrayList<SenseiSystemInfo>(parts.length);
    Map<Integer, Set<Integer>> partsMap = new HashMap<Integer, Set<Integer>>();
    Map<Integer, Node> nodeMap = new HashMap<Integer, Node>();
    Map<Integer, Future<SenseiSystemInfo>> futureMap = new HashMap<Integer, Future<SenseiSystemInfo>>();
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
    for (Node node : _nodes)
    {
      if (nodeMap.containsKey(node.getId()))
        req.setPartitions(partsMap.get(node.getId()));
      else
        req.setPartitions(node.getPartitionIds());
      if (logger.isDebugEnabled())
      {
        logger.debug("DEBUG: broker sending req part: " + req.getPartitions() + " on node: " + node);
      }
      futureMap.put(node.getId(), (Future<SenseiSystemInfo>)_networkClient.sendRequestToNode(req, node, _serializer));
    }
    for(Map.Entry<Integer, Future<SenseiSystemInfo>> entry : futureMap.entrySet())
    { 
      SenseiSystemInfo res;
      try
      {
        res = entry.getValue().get(_timeout,TimeUnit.MILLISECONDS);
        if (!nodeMap.containsKey(entry.getKey()))  // Do not count.
          res.setNumDocs(0);
        resultlist.add(res);
        if (logger.isDebugEnabled())
        {
          logger.info("DEBUG: broker receiving res part: " + (partsMap.containsKey(entry.getKey())?partsMap.get(entry.getKey()):-1)
              + " on node: " + (nodeMap.containsKey(entry.getKey())?nodeMap.get(entry.getKey()):entry.getKey())
              + " node time: " + res.getTime() +"ms remote time: " + (System.currentTimeMillis() - time) + "ms");
        }
      } catch (Exception e)
      {
        logger.error("DEBUG: broker receiving res part: " + (partsMap.containsKey(entry.getKey())?partsMap.get(entry.getKey()):-1)
            + " on node: " + (nodeMap.containsKey(entry.getKey())?nodeMap.get(entry.getKey()):entry.getKey())
            + e +" remote time: " + (System.currentTimeMillis() - time) + "ms");
      }
    }
    if (resultlist.size() == 0)
    {
      logger.error("no result received at all return empty result");
      return getEmptyResultInstance();
    }
    SenseiSystemInfo result = mergeResults(req, resultlist);
    logger.info("remote search took " + (System.currentTimeMillis() - time) + "ms");
    return result;
  }

  @Override
  public String getRouteParam(SenseiRequest req)
  {
    return req.getRouteParam();
  }

  @Override
  public SenseiSystemInfo getEmptyResultInstance()
  {
    return new SenseiSystemInfo();
  }

  @Override
  public void setTimeoutMillis(long timeoutMillis){
    _timeoutMillis = timeoutMillis;
  }

  @Override
  public long getTimeoutMillis(){
    return _timeoutMillis;
  }

  public void handleClusterConnected(Set<Node> nodes)
  {
    _loadBalancer = _loadBalancerFactory.newLoadBalancer(nodes);
    _partitions = getPartitions(nodes);
    _nodes = nodes;
    logger.info("handleClusterConnected(): Received the list of nodes from norbert " + nodes.toString());
    logger.info("handleClusterConnected(): Received the list of partitions from router " + _partitions.toString());
  }

  public void handleClusterDisconnected()
  {
    logger.info("handleClusterDisconnected() called");
    _partitions = new IntOpenHashSet();
    _nodes = Collections.EMPTY_SET;
  }

  public void handleClusterNodesChanged(Set<Node> nodes)
  {
    _loadBalancer = _loadBalancerFactory.newLoadBalancer(nodes);
    _partitions = getPartitions(nodes);
    _nodes = nodes;
    logger.info("handleClusterNodesChanged(): Received the list of nodes from norbert " + nodes.toString());
    logger.info("handleClusterNodesChanged(): Received the list of partitions from router " + _partitions.toString());
  }

  @Override
  public void handleClusterShutdown()
  {
    logger.info("handleClusterShutdown() called");
  }
}

