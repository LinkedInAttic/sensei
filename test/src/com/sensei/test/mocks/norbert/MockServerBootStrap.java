package com.sensei.test.mocks.norbert;

import com.linkedin.norbert.cluster.ClusterShutdownException;
import com.linkedin.norbert.cluster.Node;
import com.linkedin.norbert.cluster.javaapi.Cluster;
import com.linkedin.norbert.cluster.javaapi.RouterFactory;
import com.linkedin.norbert.network.javaapi.MessageHandler;
import com.linkedin.norbert.network.javaapi.NetworkClient;
import com.linkedin.norbert.network.javaapi.NetworkServer;
import com.linkedin.norbert.network.javaapi.ServerBootstrap;
import com.linkedin.norbert.network.javaapi.ServerConfig;

public class MockServerBootStrap extends ServerBootstrap {

	private final MockCluster _cluster;
	private final NetworkClient _networkClient;
	private final MessageHandler[] _msgHandlers;
	private final int _nodeId;
	private NetworkServer _networkServer;
	
	public MockServerBootStrap(ServerConfig serverConfig) {
		super(serverConfig);
		String clusterName = serverConfig.getClusterName();
		_nodeId = serverConfig.getNodeId();
		_msgHandlers = serverConfig.getMessageHandlers();
		RouterFactory routerFactory = serverConfig.getRouterFactory();
		_cluster = MockClusterHome.getCluster(clusterName);
		if (routerFactory!=null){
			_cluster.setRouterFactory(routerFactory);
		}
		_networkClient = new MockNetworkClient(_cluster);
		_networkServer = null;
	}

	@Override
	public Cluster getCluster() {
		return _cluster;
	}

	@Override
	public NetworkClient getNetworkClient() {
		return _networkClient;
	}

	@Override
	public NetworkServer getNetworkServer() {
		if (_networkServer==null){
			try{
			  Node node = _cluster.getNodeWithId(_nodeId);
			  _networkServer = new MockNetworkServer(node.getAddress(),_msgHandlers);
			}
			catch(Exception e){
				e.printStackTrace();
				_networkServer=null;
		     }
		}
		return _networkServer;
	}

	@Override
	public void shutdown() {
		try {
		  _networkClient.close();
		} 
		catch (ClusterShutdownException e) {
		  e.printStackTrace();
		}
		finally{
		  if (_networkServer!=null){
		    _networkServer.shutdown();
		  }
		  _cluster.shutdown();
		}
	}	
}
