package com.sensei.search.cluster.routing;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.IntSet;

import java.util.ArrayList;
import java.util.Random;

import com.linkedin.norbert.cluster.javaapi.Node;
import com.linkedin.norbert.network.javaapi.PartitionedLoadBalancer;

public class UniformPartitionedLoadBalancer implements PartitionedLoadBalancer<Integer> {
  
	private final Int2ObjectMap<ArrayList<Node>> _nodeMap;
	private final Random _rand;
	
	public UniformPartitionedLoadBalancer(Int2ObjectMap<ArrayList<Node>> nodeMap,Random rand){
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

  public Node nextNode(int partition)
  {
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

  /* (non-Javadoc)
   * @see com.linkedin.norbert.network.javaapi.PartitionedLoadBalancer#nextNode(java.lang.Object)
   */
  public Node nextNode(Integer partition)
  {
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
