//package com.senseidb.cluster.routing;
//
//import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
//import it.unimi.dsi.fastutil.ints.IntSet;
//
//import java.util.ArrayList;
//import java.util.List;
//import java.util.Random;
//
//import com.linkedin.norbert.javacompat.cluster.Node;
//
//public class UniformPartitionedLoadBalancer implements SenseiLoadBalancer {
//
//	private final Int2ObjectMap<ArrayList<Node>> _nodeMap;
//	private final Random _rand;
//
//	public UniformPartitionedLoadBalancer(Int2ObjectMap<ArrayList<Node>> nodeMap,Random rand){
//		_nodeMap = nodeMap;
//		_rand = rand;
//	}
//
//	public IntSet getPartitions(){
//		return _nodeMap.keySet();
//	}
//
//	public Node calculateRoute(int partition) {
//		ArrayList<Node> nodeList = _nodeMap.get(partition);
//		int size=0;
//		if (nodeList!=null && (size=nodeList.size())>0){
//			int idx = _rand.nextInt(size);
//			return nodeList.get(idx);
//		}
//		else{
//			return null;
//		}
//	}
//
//  public Node nextNode(int partition)
//  {
//    ArrayList<Node> nodeList = _nodeMap.get(partition);
//    int size=0;
//    if (nodeList!=null && (size=nodeList.size())>0){
//        int idx = _rand.nextInt(size);
//        return nodeList.get(idx);
//    }
//    else{
//        return null;
//    }
//  }
//
//  public Node nextNode(Integer partition)
//  {
//    ArrayList<Node> nodeList = _nodeMap.get(partition);
//    int size=0;
//    if (nodeList!=null && (size=nodeList.size())>0){
//        int idx = _rand.nextInt(size);
//        return nodeList.get(idx);
//    }
//    else{
//        return null;
//    }
//  }
//
//  public RoutingInfo route(String routeParam)
//  {
//    List<Node>[] nodeLists = new List[getPartitions().size()];
//    int[] partitions = new int[getPartitions().size()];
//    int[] nodegroup = new int[getPartitions().size()];
//    int i = 0;
//    for(int p : getPartitions())
//    {
//      List<Node> nodeList = new ArrayList<Node>(1);
//      nodeList.add(nextNode(p));
//      nodeLists[i] = nodeList;
//      partitions[i] = p;
//      ++i;
//    }
//    return new RoutingInfo(nodeLists, partitions, nodegroup);
//  }
//}
