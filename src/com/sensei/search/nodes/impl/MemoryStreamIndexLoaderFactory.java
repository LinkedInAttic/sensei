package com.sensei.search.nodes.impl;

import java.util.Map;

import proj.zoie.impl.indexing.MemoryStreamDataProvider;
import proj.zoie.impl.indexing.ZoieSystem;

import com.sensei.search.nodes.SenseiIndexLoader;
import com.sensei.search.nodes.SenseiIndexLoaderFactory;
import com.sensei.search.svc.api.SenseiException;

public class MemoryStreamIndexLoaderFactory<V> implements
		SenseiIndexLoaderFactory<V> {

	private Map<Integer, MemoryStreamDataProvider<V>> _memoryDataProviderMap;

	public MemoryStreamIndexLoaderFactory(
			Map<Integer, MemoryStreamDataProvider<V>> memoryDataProviderMap) {
		_memoryDataProviderMap = memoryDataProviderMap;
	}

	public SenseiIndexLoader getIndexLoader(int partition, ZoieSystem<?, V> zoie) {
		final MemoryStreamDataProvider<V> memoryDataProvider = _memoryDataProviderMap
				.get(partition);
		if (memoryDataProvider != null) {
			memoryDataProvider.setDataConsumer(zoie);
			return new SenseiIndexLoader() {

				@Override
				public void shutdown() throws SenseiException {
					memoryDataProvider.flush();
					memoryDataProvider.stop();
				}

				@Override
				public void start() throws SenseiException {
					memoryDataProvider.start();
				}

			};
		} else {
			return null;
		}
	}

}
