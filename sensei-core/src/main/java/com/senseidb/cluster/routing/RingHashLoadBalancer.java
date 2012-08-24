//package com.senseidb.cluster.routing;
//
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Set;
//import java.util.TreeMap;
//
//import org.apache.log4j.Logger;
//
//import scala.actors.threadpool.Arrays;
//
//import com.linkedin.norbert.javacompat.cluster.Node;
//
//public class RingHashLoadBalancer implements SenseiLoadBalancer
//{
//  private static Logger logger = Logger.getLogger(RingHashLoadBalancer.class);
//  private final TreeMap<Long, int[]> nodeCircleMap = new TreeMap<Long, int[]>();
//  private final int _numberOfReplicas;
//  private final HashProvider _hashingStrategy;
//  private final int[] _partitions;
//  private final List<Node>[] _nodeslist;
//
//  public RingHashLoadBalancer(HashProvider hashingStrategy, int numberOfReplicas, Set<Node> nodes)
//  {
//    _hashingStrategy = hashingStrategy;
//    _numberOfReplicas = numberOfReplicas;
//    // pton is a mapping from partition ID to a list of nodes that serve that
//    // parition
//    HashMap<Integer, List<Node>> pton = new HashMap<Integer, List<Node>>();
//    int pnodecount = 0;
//    for (Node node : nodes)
//    {
//      for (Integer partitionId : node.getPartitionIds())
//      {
//        List<Node> nodelist = pton.get(partitionId);
//        if (nodelist == null)
//        {
//          nodelist = new ArrayList<Node>();
//          pton.put(partitionId, nodelist);
//        }
//        if (!nodelist.contains(node))
//        {
//          nodelist.add(node);
//          pnodecount++;
//        }
//      }
//    }
//    int[] partitions = new int[pton.size()];
//    Integer[] partitionset = pton.keySet().toArray(new Integer[0]);
//    @SuppressWarnings("unchecked")
//    List<Node>[] nodesarray = new List[partitions.length];
//    for (int i = 0; i < partitionset.length; i++)
//    {
//      partitions[i] = partitionset[i];
//      nodesarray[i] = pton.get(partitions[i]);
//    }
//    _partitions = partitions;
//    _nodeslist = nodesarray;
//    int slots = _numberOfReplicas * pnodecount;
//    for (int r = 0; r < slots; r++)
//    {
//      int[] nodegroup = new int[_partitions.length];
//      // nodegroup has a group of nodes that each serves a partition and they
//      // collectively serve all the partitions.
//      for (int i = 0; i < _partitions.length; i++)
//      {
//        nodegroup[i] = r % _nodeslist[i].size();
//      }
//      String distKey = r + Arrays.toString(nodegroup);
//      nodeCircleMap.put(_hashingStrategy.hash(distKey), nodegroup);
//    }
//  }
//
//  public RoutingInfo route(String routeParam)
//  {
//    if (nodeCircleMap.isEmpty())
//      return null;
//
//    long hash = _hashingStrategy.hash(routeParam);
//    if (!nodeCircleMap.containsKey(hash))
//    {
//      Long k = nodeCircleMap.ceilingKey(hash);
//      hash = (k == null) ? nodeCircleMap.firstKey() : k;
//    }
//    int[] nodegroup = nodeCircleMap.get(hash);
//    if (logger.isDebugEnabled())
//    {
//      logger.debug(routeParam + " is sent to node group " + Arrays.toString(nodegroup) + " for parititions: " + Arrays.toString(_partitions));
//    }
//    return new RoutingInfo(_nodeslist, _partitions, nodegroup);
//  }
//}
