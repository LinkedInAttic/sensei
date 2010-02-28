package com.sensei.test.mocks.norbert;

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

import java.util.HashMap;
import java.util.Map;

import com.linkedin.norbert.cluster.Node;
import com.linkedin.norbert.cluster.javaapi.RouterFactory;

public class MockClusterHome {
	private static final Map<String,MockCluster> clusterMap;
	
	static{
		clusterMap = new HashMap<String,MockCluster>();
	}
	
	public static MockCluster getCluster(String clusterName){
		synchronized(clusterMap){
			MockCluster cluster = clusterMap.get(clusterName);
			if (cluster == null){
				Int2ObjectOpenHashMap<Node> nodeMap = new Int2ObjectOpenHashMap<Node>();
				cluster = new MockCluster(nodeMap);
				clusterMap.put(clusterName, cluster);
			}
			return cluster;
		}
	}
	
	public static void setRouterFactory(String clusterName,RouterFactory routerFactory){
		synchronized(clusterMap){
			MockCluster cluster = clusterMap.get(clusterName);
			if (cluster != null){
				cluster.setRouterFactory(routerFactory);
			}
		}
	}
}
