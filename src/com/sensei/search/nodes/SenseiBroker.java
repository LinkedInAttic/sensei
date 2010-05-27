package com.sensei.search.nodes;

import it.unimi.dsi.fastutil.ints.IntSet;

import java.util.Set;

import org.apache.log4j.Logger;

import com.linkedin.norbert.NorbertException;
import com.linkedin.norbert.cluster.javaapi.ClusterClient;
import com.linkedin.norbert.cluster.javaapi.ClusterListener;
import com.linkedin.norbert.cluster.javaapi.Node;
import com.linkedin.norbert.network.javaapi.PartitionedLoadBalancerFactory;
import com.linkedin.norbert.network.javaapi.PartitionedNetworkClient;
import com.sensei.search.cluster.client.SenseiNetworkClient;
import com.sensei.search.cluster.routing.UniformPartitionedLoadBalancer;
import com.sensei.search.req.SenseiRequest;
import com.sensei.search.req.SenseiResult;
import com.sensei.search.req.protobuf.SenseiRequestBPO;
import com.sensei.search.req.protobuf.SenseiRequestBPOConverter;
import com.sensei.search.req.protobuf.SenseiResultBPO;
import com.sensei.search.svc.api.SenseiException;

public class SenseiBroker implements ClusterListener  {
  private final static Logger logger = Logger.getLogger(SenseiBroker.class);
  private final SenseiNetworkClient _networkClient;

  private volatile PartitionedLoadBalancerFactory<Integer> _routerFactory = null;

  private IntSet _parts = null;
  private final SenseiRequestScatterRewriter _reqRewriter;
  private final SenseiScatterGatherHandler _scatterGatherHandler;

  public SenseiBroker(SenseiNetworkClient networkClient,
                      ClusterClient clusterClient,
                      SenseiRequestScatterRewriter reqRewriter,
                      PartitionedLoadBalancerFactory<Integer> routerFactory) throws NorbertException{
    _routerFactory = routerFactory;
    _networkClient = networkClient;
    _reqRewriter = reqRewriter;
    _scatterGatherHandler = new SenseiScatterGatherHandler(_reqRewriter);
    
    // register the request-response messages
    _networkClient.registerRequest(SenseiRequestBPO.Request.getDefaultInstance(), SenseiResultBPO.Result.getDefaultInstance());

    clusterClient.addListener(this);
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
