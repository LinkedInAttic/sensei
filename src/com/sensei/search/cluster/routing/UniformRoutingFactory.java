package com.sensei.search.cluster.routing;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;

import java.util.ArrayList;
import java.util.Random;

import com.linkedin.norbert.cluster.InvalidClusterException;
import com.linkedin.norbert.cluster.Node;
import com.linkedin.norbert.cluster.javaapi.Router;
import com.linkedin.norbert.cluster.javaapi.RouterFactory;

public class UniformRoutingFactory implements RouterFactory {
	private final Random _rand = new Random(System.nanoTime());
	
	public Router newRouter(Node[] nodes) throws InvalidClusterException {
		final Int2ObjectMap<ArrayList<Node>> nodeMap = new Int2ObjectOpenHashMap<ArrayList<Node>>();
		IntSet parts = new IntOpenHashSet();
		for (Node node : nodes){
			int[] partitions = node.getPartitions();
			for (int partition : partitions){
				parts.add(partition);
				ArrayList<Node> nodeList = nodeMap.get(partition);
				if (nodeList==null){
					nodeList=new ArrayList<Node>(nodes.length);
					nodeMap.put(partition, nodeList);
				}
				nodeList.add(node);
			}
		}
		
		return new UniformRouter(nodeMap,_rand);
	}

}
