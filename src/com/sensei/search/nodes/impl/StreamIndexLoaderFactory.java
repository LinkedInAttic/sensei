package com.sensei.search.nodes.impl;

import proj.zoie.api.Zoie;
import proj.zoie.api.ZoieVersion;
import proj.zoie.impl.indexing.StreamDataProvider;

import com.browseengine.bobo.api.BoboIndexReader;
import com.sensei.search.nodes.SenseiIndexLoader;
import com.sensei.search.nodes.SenseiIndexLoaderFactory;

public abstract class StreamIndexLoaderFactory<T,V extends ZoieVersion> implements SenseiIndexLoaderFactory<T,V> {

	@Override
	public SenseiIndexLoader getIndexLoader(int partitionId, Zoie<BoboIndexReader,T,V> dataConsumer) {
		V version = dataConsumer.getVersion();
		StreamDataProvider<T,V> dataProvider = buildStreamDataProvider(partitionId,version);
		dataProvider.setDataConsumer(dataConsumer);
		
		return new StreamingIndexLoader(dataProvider);
	}

	public abstract StreamDataProvider<T,V> buildStreamDataProvider(int partitinId,V startVersion);
}
