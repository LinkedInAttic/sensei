package com.senseidb.search.req.mapred.obsolete;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

import com.linkedin.norbert.NorbertException;
import com.linkedin.norbert.javacompat.cluster.ClusterClient;
import com.linkedin.norbert.javacompat.cluster.Node;
import com.linkedin.norbert.javacompat.network.PartitionedNetworkClient;
import com.senseidb.cluster.routing.SenseiLoadBalancerFactory;
import com.senseidb.search.node.AbstractConsistentHashBroker;
import com.senseidb.svc.api.SenseiException;

public class MapReduceBroker extends AbstractConsistentHashBroker<MapReduceRequest, SenseiMapReduceResult> {
  private final static Logger logger = Logger.getLogger(MapReduceBroker.class);
  private final SenseiLoadBalancerFactory loadBalancerFactory;
  private long _timeoutMillis;

  public MapReduceBroker(PartitionedNetworkClient<Integer> networkClient, ClusterClient clusterClient,
      SenseiLoadBalancerFactory loadBalancerFactory) throws NorbertException {
    super(networkClient, MapReduceSenseiService.SERIALIZER);
    this.loadBalancerFactory = loadBalancerFactory;
    clusterClient.addListener(this);
    logger.info("created broker instance " + networkClient + " " + clusterClient + " " + loadBalancerFactory);
  }

  @Override
  public SenseiMapReduceResult getEmptyResultInstance() {
   
    return new SenseiMapReduceResult();
  }
@Override
  public SenseiMapReduceResult browse(MapReduceRequest req) throws SenseiException {
    long time = System.currentTimeMillis();
    SenseiMapReduceResult ret = super.browse(req);
    ret.setTime(System.currentTimeMillis() - time);
    return ret;
  }
  @Override
  public SenseiMapReduceResult mergeResults(MapReduceRequest request, List<SenseiMapReduceResult> resultList) {
    int size = 0;
    for (SenseiMapReduceResult reduceResult : resultList) {
      size += reduceResult.getMapResults().size();
    }
    List<Object> mapRes = new ArrayList<Object>(size);
    for (SenseiMapReduceResult reduceResult : resultList) {
      mapRes.addAll(reduceResult.getMapResults());
    }
    
    return (SenseiMapReduceResult) new SenseiMapReduceResult().setReduceResult(request.getMapReduceJob().reduce(mapRes));
  }

  @Override
  public String getRouteParam(MapReduceRequest req) {
    
    return req.getRouteParam();
  }

  @Override
  public void setTimeoutMillis(long timeoutMillis) {
    _timeoutMillis = timeoutMillis;
  }

  @Override
  public long getTimeoutMillis() {
    return _timeoutMillis;
  }
  @Override
  public MapReduceRequest customizeRequest(MapReduceRequest request) {
    
    return request;
  }
  public void handleClusterConnected(Set<Node> nodes) {
    _loadBalancer = loadBalancerFactory.newLoadBalancer(nodes);
    _partitions = getPartitions(nodes);
    logger.info("handleClusterConnected(): Received the list of nodes from norbert " + nodes.toString());
    logger.info("handleClusterConnected(): Received the list of partitions from router " + _partitions.toString());
  }

  public void handleClusterDisconnected() {
    logger.info("handleClusterDisconnected() called");
    _partitions = new IntOpenHashSet();
  }

  public void handleClusterNodesChanged(Set<Node> nodes) {
    _loadBalancer = loadBalancerFactory.newLoadBalancer(nodes);
    _partitions = getPartitions(nodes);
    logger.info("handleClusterNodesChanged(): Received the list of nodes from norbert " + nodes.toString());
    logger.info("handleClusterNodesChanged(): Received the list of partitions from router " + _partitions.toString());
  }

  @Override
  public void handleClusterShutdown() {
    logger.info("handleClusterShutdown() called");
  }
}
