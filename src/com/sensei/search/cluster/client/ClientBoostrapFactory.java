package com.sensei.search.cluster.client;

import com.linkedin.norbert.network.javaapi.ClientBootstrap;
import com.linkedin.norbert.network.javaapi.ClientConfig;

public interface ClientBoostrapFactory {
	ClientBootstrap getClientBootstrap(ClientConfig config);
	
	public static class DefaultClientBoostrapFactory implements ClientBoostrapFactory{

		@Override
		public ClientBootstrap getClientBootstrap(ClientConfig config) {
			return new ClientBootstrap(config);
		}
		
	}
}
