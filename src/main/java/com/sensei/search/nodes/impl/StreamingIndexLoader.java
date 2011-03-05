package com.sensei.search.nodes.impl;

import proj.zoie.impl.indexing.StreamDataProvider;

import com.sensei.search.nodes.SenseiIndexLoader;
import com.sensei.search.svc.api.SenseiException;

public class StreamingIndexLoader implements SenseiIndexLoader {

	private final StreamDataProvider _dataProvider;
	public StreamingIndexLoader(StreamDataProvider dataProvider){
		_dataProvider = dataProvider;
	}
	
	@Override
	public void shutdown() throws SenseiException {
		_dataProvider.stop();
	}

	@Override
	public void start() throws SenseiException {
		_dataProvider.start();
	}
}
