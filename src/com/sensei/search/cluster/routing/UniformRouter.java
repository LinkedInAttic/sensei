package com.sensei.search.cluster.routing;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.IntSet;

import java.util.ArrayList;
import java.util.Random;

import com.linkedin.norbert.cluster.Node;
import com.linkedin.norbert.cluster.javaapi.Router;

public class UniformRouter implements Router {
	private final Int2ObjectMap<ArrayList<Node>> _nodeMap;
	private final Random _rand;
	public UniformRouter(Int2ObjectMap<ArrayList<Node>> nodeMap,Random rand){
		_nodeMap = nodeMap;
		_rand = rand;
	}
	
	public IntSet getPartitions(){
		return _nodeMap.keySet();
	}
	
	public Node calculateRoute(int partition) {
		ArrayList<Node> nodeList = _nodeMap.get(partition);
		int size=0;
		if (nodeList!=null && (size=nodeList.size())>0){
			int idx = _rand.nextInt(size);
			return nodeList.get(idx);
		}
		else{
			return null;
		}
	}

}
