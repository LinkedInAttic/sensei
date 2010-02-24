package com.sensei.search.nodes;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;

import java.util.List;

import org.apache.lucene.queryParser.QueryParser;

import proj.zoie.api.IndexReaderFactory;
import proj.zoie.api.ZoieIndexReader;

import com.browseengine.bobo.api.BoboIndexReader;
import com.sensei.search.req.RuntimeFacetHandlerFactory;

public class SenseiSearchContext {
	private final QueryParser _qparser;
	private final Int2ObjectMap<IndexReaderFactory<ZoieIndexReader<BoboIndexReader>>> _partReaderMap;
	private final List<RuntimeFacetHandlerFactory<?>> _runtimeFacetHandlerFactories;
	
	public SenseiSearchContext(QueryParser qparser,Int2ObjectMap<IndexReaderFactory<ZoieIndexReader<BoboIndexReader>>> partReaderMap,List<RuntimeFacetHandlerFactory<?>> runtimeFacetHandlerFactories){
		_qparser = qparser;
		_partReaderMap = partReaderMap;
		_runtimeFacetHandlerFactories = runtimeFacetHandlerFactories;
	}

	public QueryParser getQparser() {
		return _qparser;
	}

	public IndexReaderFactory<ZoieIndexReader<BoboIndexReader>> getIdxReaderFactory(int partition) {
		return _partReaderMap == null ? null : _partReaderMap.get(partition);
	}
	
	public Int2ObjectMap<IndexReaderFactory<ZoieIndexReader<BoboIndexReader>>>  getPartitionReaderMap(){
		return _partReaderMap;
	}

	public List<RuntimeFacetHandlerFactory<?>> getRuntimeFacetHandlerFactories() {
		return _runtimeFacetHandlerFactories;
	}
}
