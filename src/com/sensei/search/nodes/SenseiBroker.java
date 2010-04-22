package com.sensei.search.nodes;

import java.util.Arrays;

import it.unimi.dsi.fastutil.ints.IntSet;

import org.apache.log4j.Logger;

import com.linkedin.norbert.NorbertException;
import com.linkedin.norbert.cluster.Node;
import com.linkedin.norbert.cluster.javaapi.ClusterClient;
import com.linkedin.norbert.cluster.javaapi.ClusterListener;
import com.linkedin.norbert.network.javaapi.PartitionedLoadBalancerFactory;
import com.linkedin.norbert.network.javaapi.PartitionedNetworkClient;
import com.sensei.search.cluster.routing.UniformPartitionedLoadBalancer;
import com.sensei.search.req.SenseiRequest;
import com.sensei.search.req.SenseiResult;
import com.sensei.search.req.protobuf.SenseiRequestBPO;
import com.sensei.search.req.protobuf.SenseiRequestBPOConverter;
import com.sensei.search.svc.api.SenseiException;

public class SenseiBroker implements ClusterListener  {
  private final static Logger logger = Logger.getLogger(SenseiBroker.class);
  private final ClusterClient _cluster;

  private final PartitionedNetworkClient<Integer> _networkClient;

  private volatile PartitionedLoadBalancerFactory<Integer> _routerFactory = null;

  private IntSet _parts = null;
  private final SenseiRequestScatterRewriter _reqRewriter;
  private final SenseiScatterGatherHandler _scatterGatherHandler;

  public SenseiBroker(ClusterClient cluster, PartitionedNetworkClient<Integer> networkClient,SenseiRequestScatterRewriter reqRewriter,
                      PartitionedLoadBalancerFactory<Integer> routerFactory) throws NorbertException{
    _cluster = cluster;
    _cluster.addListener(this);
    _routerFactory = routerFactory;
    _networkClient = networkClient;
    _reqRewriter = reqRewriter;
    _scatterGatherHandler = new SenseiScatterGatherHandler(_reqRewriter);
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
      Integer[] partToSend = req.getPartitions();
      if (partToSend==null){
        partToSend = partitions.toArray(new Integer[partitions.size()]);
      }
      SenseiResult res;
      if (partToSend.length>0){
        //		    res = networkClient.sendMessage(partitions.toIntArray(), msg, _scatterGatherHandler);
        Integer[] partitionIds = new Integer[partitions.size()];
        int[] partitionInts = partitions.toIntArray();
        for(int index = 0; index < partitions.size(); index++)
        {
          partitionIds[index] = partitionInts[index];
        }
//       res = networkClient.sendMessage(partitionIds, msg, _scatterGatherHandler);
        res = networkClient.sendMessage(Arrays.asList(partitionIds), msg, _scatterGatherHandler);
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
  }

  /* (non-Javadoc)
   * @see com.linkedin.norbert.cluster.javaapi.ClusterListener#handleClusterConnected(com.linkedin.norbert.cluster.Node[])
   */
  public void handleClusterConnected(Node[] nodes)
  {
    UniformPartitionedLoadBalancer router = (UniformPartitionedLoadBalancer) _routerFactory.newLoadBalancer(nodes);
    _parts = router.getPartitions();
  }

  /* (non-Javadoc)
   * @see com.linkedin.norbert.cluster.javaapi.ClusterListener#handleClusterDisconnected()
   */
  public void handleClusterDisconnected()
  {

  }

  /* (non-Javadoc)
   * @see com.linkedin.norbert.cluster.javaapi.ClusterListener#handleClusterNodesChanged(com.linkedin.norbert.cluster.Node[])
   */
  public void handleClusterNodesChanged(Node[] nodes)
  {
    UniformPartitionedLoadBalancer router = (UniformPartitionedLoadBalancer) _routerFactory.newLoadBalancer(nodes);
    _parts = router.getPartitions();
  }

  /* (non-Javadoc)
   * @see com.linkedin.norbert.cluster.javaapi.ClusterListener#handleClusterShutdown()
   */
  public void handleClusterShutdown()
  {

  }
}
