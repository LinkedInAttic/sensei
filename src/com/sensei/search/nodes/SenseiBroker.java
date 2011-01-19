package com.sensei.search.nodes;

import it.unimi.dsi.fastutil.ints.IntSet;

import java.util.Set;

import org.apache.log4j.Logger;

import com.linkedin.norbert.NorbertException;
import com.linkedin.norbert.javacompat.cluster.ClusterClient;
import com.linkedin.norbert.javacompat.cluster.ClusterListener;
import com.linkedin.norbert.javacompat.cluster.Node;
import com.linkedin.norbert.javacompat.network.PartitionedLoadBalancerFactory;
import com.linkedin.norbert.javacompat.network.PartitionedNetworkClient;
import com.sensei.search.cluster.routing.UniformPartitionedLoadBalancer;
import com.sensei.search.req.SenseiRequest;
import com.sensei.search.req.SenseiResult;
import com.sensei.search.req.SenseiSystemInfo;
import com.sensei.search.req.protobuf.SenseiRequestBPO;
import com.sensei.search.req.protobuf.SenseiRequestBPOConverter;
import com.sensei.search.req.protobuf.SenseiResultBPO;
import com.sensei.search.req.protobuf.SenseiSysRequestBPO;
import com.sensei.search.req.protobuf.SenseiSysRequestBPOConverter;
import com.sensei.search.req.protobuf.SenseiSysResultBPO;
import com.sensei.search.svc.api.SenseiException;

public class SenseiBroker implements ClusterListener  {
  private final static Logger logger = Logger.getLogger(SenseiBroker.class);
  private final PartitionedNetworkClient<Integer> _networkClient;

  private volatile PartitionedLoadBalancerFactory<Integer> _routerFactory = null;

  private IntSet _parts = null;
  private final SenseiRequestScatterRewriter _reqRewriter;
  private final SenseiScatterGatherHandler _scatterGatherHandler;
  private final SenseiSysScatterGatherHandler _sysScatterGatherHandler;

  public SenseiBroker(PartitionedNetworkClient<Integer> networkClient,
                      ClusterClient clusterClient,
                      SenseiRequestScatterRewriter reqRewriter,
                      PartitionedLoadBalancerFactory<Integer> routerFactory) throws NorbertException{
    _routerFactory = routerFactory;
    _networkClient = networkClient;
    _reqRewriter = reqRewriter;
    _scatterGatherHandler = new SenseiScatterGatherHandler(_reqRewriter);
    _sysScatterGatherHandler = new SenseiSysScatterGatherHandler();
    
    // register the request-response messages
    _networkClient.registerRequest(SenseiSysRequestBPO.SysRequest.getDefaultInstance(), SenseiSysResultBPO.SysResult.getDefaultInstance());
    _networkClient.registerRequest(SenseiRequestBPO.Request.getDefaultInstance(), SenseiResultBPO.Result.getDefaultInstance());

    clusterClient.addListener(this);
  }
  
  public void setTimeoutMillis(long timeoutMillis){
    _scatterGatherHandler.setTimeoutMillis(timeoutMillis);
  }
  
  public long getTimeoutMillis(){
    return _scatterGatherHandler.getTimeoutMillis();
  }

  public SenseiResult browse(SenseiRequest req) throws SenseiException{
    if(_parts == null)
      throw new SenseiException("Browse called before cluster is connected!");
    try {
      return doBrowse(_networkClient,req, _parts);
    } catch (Exception e) {
      throw new SenseiException(e.getMessage(),e);
    }
  }

  private SenseiResult doBrowse(PartitionedNetworkClient<Integer> networkClient,SenseiRequest req,IntSet partitions) throws Exception{
    if (partitions!=null && (partitions.size())>0){
      SenseiRequestBPO.Request msg = SenseiRequestBPOConverter.convert(req);
      Set<Integer> partToSend = req.getPartitions();
      if (partToSend == null){
        partToSend = partitions;
      }
      SenseiResult res;
      if (partToSend.size() > 0){
        res = networkClient.sendMessage(partitions, msg, _scatterGatherHandler);
      }
      else{
        res = new SenseiResult();  
      }
      return res;
    }
    else{
      logger.warn("no server exist to handle request.");
      return new SenseiResult();
    }
  }

  public SenseiSystemInfo getSystemInfo() throws SenseiException {
    if(_parts == null)
      throw new SenseiException("getSystemInfo called before cluster is connected!");
    try {
      return doGetSystemInfo(_networkClient, _parts);
    } catch (Exception e) {
      throw new SenseiException(e.getMessage(),e);
    }
  }

  private SenseiSystemInfo doGetSystemInfo(PartitionedNetworkClient<Integer> networkClient, IntSet partitions) throws Exception {
    if (partitions!=null && (partitions.size())>0){
      SenseiSysRequestBPO.SysRequest msg = SenseiSysRequestBPOConverter.convert(new SenseiRequest());
      SenseiSystemInfo res = networkClient.sendMessage(partitions, msg, _sysScatterGatherHandler);
      return res;
    }
    else{
      logger.warn("no server exist to handle request.");
      return new SenseiSystemInfo();
    }
  }

  public void shutdown(){
    logger.info("shutting down broker...");
  }

  /* (non-Javadoc)
   * @see com.linkedin.norbert.cluster.javaapi.ClusterListener#handleClusterConnected(com.linkedin.norbert.cluster.Node[])
   */
  public void handleClusterConnected(Set<Node> nodes)
  {
    UniformPartitionedLoadBalancer router = (UniformPartitionedLoadBalancer) _routerFactory.newLoadBalancer(nodes);
    _parts = router.getPartitions();
    logger.info("handleClusterConnected(): Received the list of nodes from norbert " + nodes.toString());
    logger.info("handleClusterConnected(): Received the list of partitions from router " + _parts.toString());
  }

  /* (non-Javadoc)
   * @see com.linkedin.norbert.cluster.javaapi.ClusterListener#handleClusterDisconnected()
   */
  public void handleClusterDisconnected()
  {
    logger.info("handleClusterDisconnected() called");
  }

  /* (non-Javadoc)
   * @see com.linkedin.norbert.cluster.javaapi.ClusterListener#handleClusterNodesChanged(com.linkedin.norbert.cluster.Node[])
   */
  public void handleClusterNodesChanged(Set<Node> nodes)
  {
    UniformPartitionedLoadBalancer router = (UniformPartitionedLoadBalancer) _routerFactory.newLoadBalancer(nodes);
    _parts = router.getPartitions();
    logger.info("handleClusterNodesChanged(): Received the list of nodes from norbert " + nodes.toString());
    logger.info("handleClusterNodesChanged(): Received the list of partitions from router " + _parts.toString());
  }

  /* (non-Javadoc)
   * @see com.linkedin.norbert.cluster.javaapi.ClusterListener#handleClusterShutdown()
   */
  public void handleClusterShutdown()
  {
    logger.info("handleClusterShutdown() called");

  }
}
