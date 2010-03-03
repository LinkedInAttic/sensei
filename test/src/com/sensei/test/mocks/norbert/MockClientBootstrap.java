package com.sensei.test.mocks.norbert;

import com.linkedin.norbert.cluster.ClusterShutdownException;
import com.linkedin.norbert.cluster.javaapi.Cluster;
import com.linkedin.norbert.cluster.javaapi.RouterFactory;
import com.linkedin.norbert.network.javaapi.ClientBootstrap;
import com.linkedin.norbert.network.javaapi.ClientConfig;
import com.linkedin.norbert.network.javaapi.NetworkClient;

public class MockClientBootstrap extends ClientBootstrap {

	private final MockCluster _cluster;
	private final MockNetworkClient _networkClient;
	public MockClientBootstrap(ClientConfig clientConfig) {
		super(clientConfig);
		String clusterName = clientConfig.getClusterName();
		RouterFactory routerFactory = clientConfig.getRouterFactory();
		_cluster = MockClusterHome.getCluster(clusterName);
		if (routerFactory!=null){
			_cluster.setRouterFactory(routerFactory);
		}
		_networkClient = new MockNetworkClient(_cluster);
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
	public void shutdown() {
		try {
			_networkClient.close();
		} catch (ClusterShutdownException e) {
			e.printStackTrace();
		}
		finally{
			_cluster.shutdown();
		}
	}

}
