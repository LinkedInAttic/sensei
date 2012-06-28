package com.senseidb.search.node;

import com.linkedin.norbert.javacompat.network.RequestBuilder;
import com.linkedin.norbert.network.ResponseIterator;
import com.linkedin.norbert.network.common.TimeoutIterator;
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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.linkedin.norbert.NorbertException;
import com.linkedin.norbert.javacompat.cluster.ClusterClient;
import com.linkedin.norbert.javacompat.cluster.Node;
import com.linkedin.norbert.javacompat.network.PartitionedNetworkClient;
import com.senseidb.cluster.routing.RoutingInfo;
import com.senseidb.search.req.SenseiRequest;
import com.senseidb.search.req.SenseiSystemInfo;
import com.senseidb.svc.impl.SysSenseiCoreServiceImpl;

public class SenseiSysBroker extends AbstractConsistentHashBroker<SenseiRequest, SenseiSystemInfo>
{
  private final static Logger logger = Logger.getLogger(SenseiSysBroker.class);
  private final static long TIMEOUT_MILLIS = 8000L;
  private long _timeoutMillis = TIMEOUT_MILLIS;
  private final Comparator<String> _versionComparator;
  private final boolean allowPartialMerge;

  protected Set<Node> _nodes = Collections.EMPTY_SET;

  public SenseiSysBroker(PartitionedNetworkClient<String> networkClient, ClusterClient clusterClient, Comparator<String> versionComparator, boolean allowPartialMerge) throws NorbertException
  {
    super(networkClient, SysSenseiCoreServiceImpl.JAVA_SERIALIZER);
    _versionComparator = versionComparator;
    this.allowPartialMerge = allowPartialMerge;
    clusterClient.addListener(this);
    logger.info("created broker instance " + networkClient + " " + clusterClient);
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
  protected List<SenseiSystemInfo> doCall(final SenseiRequest req) throws ExecutionException {
    final List<SenseiSystemInfo> resultList = new ArrayList<SenseiSystemInfo>();
    List<Future<SenseiSystemInfo>> futures = new ArrayList<Future<SenseiSystemInfo>>(_nodes.size());
    for(Node n : _nodes)
    {
      futures.add(_networkClient.sendRequestToNode(req, n, _serializer));
    }
    for(Future<SenseiSystemInfo> future : futures)
    {
      try
      {
        resultList.add(future.get(2000L, TimeUnit.MILLISECONDS));
      }
      catch(Exception e)
      {
        logger.error("Failed to get the sysinfo", e);
      }
    }

    logger.debug(String.format("There are %d responses", resultList.size()));

    return resultList;

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

  @Override
  public boolean allowPartialMerge() {
    return allowPartialMerge;
  }
}

