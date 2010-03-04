package com.sensei.search.nodes;

import com.linkedin.norbert.network.javaapi.ServerBootstrap;
import com.linkedin.norbert.network.javaapi.ServerConfig;

public interface ServerBootstrapFactory {
	ServerBootstrap getServerBootstrap(ServerConfig config);
	
	public static class DefaultServerBootstrapFactory implements ServerBootstrapFactory{

		@Override
		public ServerBootstrap getServerBootstrap(ServerConfig config) {
			return new ServerBootstrap(config);
		}
		
	}
}
