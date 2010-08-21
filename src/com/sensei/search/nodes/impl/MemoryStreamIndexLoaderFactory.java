package com.sensei.search.nodes.impl;

import java.util.Map;

import proj.zoie.api.ZoieVersion;
import proj.zoie.impl.indexing.MemoryStreamDataProvider;
import proj.zoie.impl.indexing.ZoieSystem;

import com.sensei.search.nodes.SenseiIndexLoader;
import com.sensei.search.nodes.SenseiIndexLoaderFactory;
import com.sensei.search.svc.api.SenseiException;

public class MemoryStreamIndexLoaderFactory<T,V extends ZoieVersion> implements
		SenseiIndexLoaderFactory<T,V> {

	private Map<Integer, MemoryStreamDataProvider<T,V>> _memoryDataProviderMap;

	public MemoryStreamIndexLoaderFactory(
			Map<Integer, MemoryStreamDataProvider<T,V>> memoryDataProviderMap) {
		_memoryDataProviderMap = memoryDataProviderMap;
	}

	public SenseiIndexLoader getIndexLoader(int partition, ZoieSystem<?, T,V> zoie) {
		final MemoryStreamDataProvider<T,V> memoryDataProvider = _memoryDataProviderMap
				.get(partition);
		if (memoryDataProvider != null) {
			CompositeDataConsumer<T,V> consumer = (CompositeDataConsumer<T,V>)memoryDataProvider.getDataConsumer();
			if (consumer==null){
				consumer = new CompositeDataConsumer<T,V>();
				memoryDataProvider.setDataConsumer(consumer);
			}
			consumer.addDataConsumer(zoie);
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
