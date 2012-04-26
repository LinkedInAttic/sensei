//package com.senseidb.cluster.routing;
//
//import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
//import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
//import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
//import it.unimi.dsi.fastutil.ints.IntSet;
//
//import java.util.ArrayList;
//import java.util.Random;
//import java.util.Set;
//
//import com.linkedin.norbert.cluster.InvalidClusterException;
//import com.linkedin.norbert.javacompat.cluster.Node;
//
//public class UniformPartitionedRoutingFactory implements SenseiLoadBalancerFactory {
//	private final Random _rand = new Random(System.nanoTime());
//
//  public SenseiLoadBalancer newLoadBalancer(Set<Node> nodes) throws InvalidClusterException
//  {
//    final Int2ObjectMap<ArrayList<Node>> nodeMap = new Int2ObjectOpenHashMap<ArrayList<Node>>();
//    IntSet parts = new IntOpenHashSet();
//    for (Node node : nodes){
//        Set<Integer> partitions = node.getPartitionIds();
//        for (Integer partition : partitions){
//            parts.add(partition);
//            ArrayList<Node> nodeList = nodeMap.get(partition);
//            if (nodeList==null){
//                nodeList=new ArrayList<Node>(nodes.size());
//                nodeMap.put(partition, nodeList);
//            }
//            nodeList.add(node);
//        }
//    }
//    return new UniformPartitionedLoadBalancer(nodeMap,_rand);
//  }
//}
