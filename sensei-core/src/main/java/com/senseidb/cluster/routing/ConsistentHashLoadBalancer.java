//package com.senseidb.cluster.routing;
//
//import com.linkedin.norbert.javacompat.cluster.Node;
//import java.util.ArrayList;
//import java.util.HashSet;
//import java.util.List;
//import java.util.Map;
//import java.util.NavigableMap;
//import java.util.Set;
//import java.util.TreeMap;
//import org.apache.log4j.Logger;
//
//
///**
// * A {@link SenseiLoadBalancer} that provides consistent hash behavior for all partitions with one lookup per routing.
// */
//public class ConsistentHashLoadBalancer implements SenseiLoadBalancer
//{
//  private static Logger logger = Logger.getLogger(ConsistentHashLoadBalancer.class);
//
//  private final HashProvider _hashProvider;
//  private final NavigableMap<Long, RoutingInfo> _routingMap;
//
//  public ConsistentHashLoadBalancer(HashProvider hashProvider, int bucketCount, Set<Node> nodes)
//  {
//    _hashProvider = hashProvider;
//    _routingMap = new TreeMap<Long, RoutingInfo>();
//
//    // Gather set of nodes for each partition
//    Map<Integer, Set<Node>> partitionNodes = new TreeMap<Integer, Set<Node>>();
//    for (Node node : nodes)
//    {
//      for (Integer partId : node.getPartitionIds())
//      {
//        Set<Node> partNodes = partitionNodes.get(partId);
//        if (partNodes == null)
//        {
//          partNodes = new HashSet<Node>();
//          partitionNodes.put(partId, partNodes);
//        }
//        partNodes.add(node);
//      }
//    }
//
//    // Build the common data structure shared among all RoutingInfo
//    int maxSize = 0;
//    int[] partitions = new int[partitionNodes.size()];
//    @SuppressWarnings("unchecked")
//    List<Node>[] nodeLists = new List[partitions.length];
//    int idx = 0;
//    for (Map.Entry<Integer, Set<Node>> entry : partitionNodes.entrySet())
//    {
//      partitions[idx] = entry.getKey();
//      nodeLists[idx] = new ArrayList<Node>(entry.getValue());
//      if (maxSize < nodeLists[idx].size()) {
//        maxSize = nodeLists[idx].size();
//      }
//      idx++;
//    }
//
//    // Builds individual ring for each partitions
//    Map<Integer, NavigableMap<Long, Integer>> rings = new TreeMap<Integer, NavigableMap<Long, Integer>>();
//    for (int i = 0; i < partitions.length; i++)
//    {
//      Integer partId = partitions[i];
//      NavigableMap<Long, Integer> ring = rings.get(partId);
//      if (ring == null)
//      {
//        ring = new TreeMap<Long, Integer>();
//        rings.put(partId, ring);
//      }
//
//      // Put points in ring. BucketCount points per node.
//      for (int j = 0; j < nodeLists[i].size(); j++)
//      {
//        for (int k = 0; k < bucketCount; k++)
//        {
//          ring.put(hashProvider.hash(String.format("node-%d-%d", nodeLists[i].get(j).getId(), k)), j);
//        }
//      }
//    }
//
//    // Generate points and gather node for each partition on each point
//    for (int slot = 0; slot < bucketCount * maxSize; slot++)
//    {
//      Long point = hashProvider.hash(String.format("ring-%d", slot));
//
//      // Choice of node for each partition
//      int[] nodeChoices = new int[partitions.length];
//      for (int i = 0; i < partitions.length; i++)
//      {
//        nodeChoices[i] = lookup(rings.get(partitions[i]), point);
//      }
//
//      _routingMap.put(point, new RoutingInfo(nodeLists, partitions, nodeChoices));
//    }
//  }
//
//  @Override
//  public RoutingInfo route(String routeParam)
//  {
//    if (_routingMap.isEmpty())
//    {
//      return null;
//    }
//
//    RoutingInfo result = lookup(_routingMap, _hashProvider.hash(routeParam));
//
//    if (logger.isDebugEnabled())
//    {
//      logger.debug(routeParam + " is sent to " + result.toString());
//    }
//
//    return result;
//  }
//
//  private <K, V> V lookup(NavigableMap<K, V> ring, K key)
//  {
//    V result = ring.get(key);
//    if (result == null)
//    {       // Not a direct match
//      Map.Entry<K, V> entry = ring.ceilingEntry(key);
//      result = (entry == null) ? ring.firstEntry().getValue() : entry.getValue();
//    }
//
//    return result;
//  }
//
//  @Override
//  public String toString()
//  {
//    return _routingMap.toString();
//  }
//}
