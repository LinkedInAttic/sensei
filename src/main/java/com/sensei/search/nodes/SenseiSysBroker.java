package com.sensei.search.nodes;

import com.sensei.search.req.protobuf.SenseiSysRequestSerializer;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;

import java.util.Comparator;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

import com.linkedin.norbert.NorbertException;
import com.linkedin.norbert.javacompat.cluster.ClusterClient;
import com.linkedin.norbert.javacompat.cluster.Node;
import com.linkedin.norbert.javacompat.network.PartitionedNetworkClient;
import com.sensei.search.cluster.routing.SenseiLoadBalancerFactory;
import com.sensei.search.req.SenseiRequest;
import com.sensei.search.req.SenseiSystemInfo;
import com.sensei.search.req.protobuf.SenseiSysRequestBPO;
import com.sensei.search.req.protobuf.SenseiSysRequestBPOConverter;
import com.sensei.search.req.protobuf.SenseiSysResultBPO;

public class SenseiSysBroker extends AbstractConsistentHashBroker<SenseiRequest, SenseiSystemInfo>
{
  private final static Logger logger = Logger.getLogger(SenseiSysBroker.class);
  private final static long TIMEOUT_MILLIS = 8000L;

  private long _timeoutMillis = TIMEOUT_MILLIS;
  private final Comparator<String> _versionComparator;

  public SenseiSysBroker(PartitionedNetworkClient<Integer> networkClient, ClusterClient clusterClient,
      SenseiLoadBalancerFactory loadBalancerFactory, Comparator<String> versionComparator) throws NorbertException
  {
    super(networkClient, clusterClient, loadBalancerFactory, SenseiSysRequestSerializer.getInstance());
    _versionComparator = versionComparator;
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
      if (result.getLastModified() < res.getLastModified())
        result.setLastModified(res.getLastModified());
      if (result.getVersion() == null || _versionComparator.compare(result.getVersion(), res.getVersion()) < 0)
        result.setVersion(res.getVersion());
      if (res.getFacetInfos() != null)
        result.setFacetInfos(res.getFacetInfos());
      if (res.getClusterInfo() != null) {
        if (result.getClusterInfo() != null)
          result.getClusterInfo().putAll(res.getClusterInfo());
        else
          result.setClusterInfo(res.getClusterInfo());
      }
    }

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
  @Override
  public void setTimeoutMillis(long timeoutMillis){
    _timeoutMillis = timeoutMillis;
  }

  @Override
  public long getTimeoutMillis(){
    return _timeoutMillis;
  }

  private IntSet getPartitions(Set<Node> nodes)
  {
    IntSet partitionSet = new IntOpenHashSet();
    for (Node n : nodes)
    {
      partitionSet.addAll(n.getPartitionIds());
    }
    return partitionSet;
  }

  public void handleClusterConnected(Set<Node> nodes)
  {
    _loadBalancer = _loadBalancerFactory.newLoadBalancer(nodes);
    _partitions = getPartitions(nodes);
    logger.info("handleClusterConnected(): Received the list of nodes from norbert " + nodes.toString());
    logger.info("handleClusterConnected(): Received the list of partitions from router " + _partitions.toString());
  }

  public void handleClusterDisconnected()
  {
    logger.info("handleClusterDisconnected() called");
    _partitions = new IntOpenHashSet();
  }

  public void handleClusterNodesChanged(Set<Node> nodes)
  {
    _loadBalancer = _loadBalancerFactory.newLoadBalancer(nodes);
    _partitions = getPartitions(nodes);
    logger.info("handleClusterNodesChanged(): Received the list of nodes from norbert " + nodes.toString());
    logger.info("handleClusterNodesChanged(): Received the list of partitions from router " + _partitions.toString());
  }

  @Override
  public void handleClusterShutdown()
  {
    logger.info("handleClusterShutdown() called");
  }
}

