package com.sensei.test.mocks;

import com.linkedin.norbert.network.javaapi.ServerBootstrap;
import com.linkedin.norbert.network.javaapi.ServerConfig;
import com.sensei.search.nodes.ServerBootstrapFactory;
import com.sensei.test.mocks.norbert.MockServerBootStrap;

public class MockServerBootstrapFactory implements ServerBootstrapFactory {

	@Override
	public ServerBootstrap getServerBootstrap(ServerConfig config) {
		return new MockServerBootStrap(config);
	}

}
