package com.sensei.test.mocks;

import com.linkedin.norbert.network.javaapi.ClientBootstrap;
import com.linkedin.norbert.network.javaapi.ClientConfig;
import com.sensei.search.cluster.client.ClientBoostrapFactory;
import com.sensei.test.mocks.norbert.MockClientBootstrap;

public class MockClientBootstrapFactory implements ClientBoostrapFactory {

	@Override
	public ClientBootstrap getClientBootstrap(ClientConfig config) {
		return new MockClientBootstrap(config);
	}
}
