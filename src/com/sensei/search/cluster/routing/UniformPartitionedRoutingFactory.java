package com.sensei.search.cluster.routing;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;

import java.util.ArrayList;
import java.util.Random;
import java.util.Set;

import com.linkedin.norbert.cluster.InvalidClusterException;
import com.linkedin.norbert.cluster.javaapi.Node;
import com.linkedin.norbert.network.javaapi.PartitionedLoadBalancer;
import com.linkedin.norbert.network.javaapi.PartitionedLoadBalancerFactory;

public class UniformPartitionedRoutingFactory implements PartitionedLoadBalancerFactory<Integer> {
	private final Random _rand = new Random(System.nanoTime());
	
  /* (non-Javadoc)
   * @see com.linkedin.norbert.network.javaapi.PartitionedLoadBalancerFactory#newLoadBalancer(com.linkedin.norbert.cluster.Node[])
   */
  public PartitionedLoadBalancer<Integer> newLoadBalancer(Set<Node> nodes) throws InvalidClusterException
  {
    final Int2ObjectMap<ArrayList<Node>> nodeMap = new Int2ObjectOpenHashMap<ArrayList<Node>>();
    IntSet parts = new IntOpenHashSet();
    for (Node node : nodes){
        Set<Integer> partitions = node.getPartitions();
        for (Integer partition : partitions){
            parts.add(partition);
            ArrayList<Node> nodeList = nodeMap.get(partition);
            if (nodeList==null){
                nodeList=new ArrayList<Node>(nodes.size());
                nodeMap.put(partition, nodeList);
            }
            nodeList.add(node);
        }
    }    
    return new UniformPartitionedLoadBalancer(nodeMap,_rand);
  }
}
