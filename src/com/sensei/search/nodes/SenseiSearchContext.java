package com.sensei.search.nodes;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.DefaultSimilarity;
import org.apache.lucene.util.Version;

import proj.zoie.api.IndexReaderFactory;
import proj.zoie.api.ZoieIndexReader;
import proj.zoie.api.indexing.ZoieIndexableInterpreter;
import proj.zoie.impl.indexing.ZoieSystem;

import com.browseengine.bobo.api.BoboIndexReader;
import com.sensei.search.req.RuntimeFacetHandlerFactory;

public class SenseiSearchContext {
	private final QueryParser _qparser;
	private final Map<Integer,IndexReaderFactory<ZoieIndexReader<BoboIndexReader>>> _partReaderMap;
	private final List<RuntimeFacetHandlerFactory<?>> _runtimeFacetHandlerFactories;
	
	private static <T> IndexReaderFactory<ZoieIndexReader<BoboIndexReader>> buildReaderFactory(File file,ZoieIndexableInterpreter<T> interpreter){
		ZoieSystem<BoboIndexReader,T> zoieSystem = new ZoieSystem<BoboIndexReader,T>(file,interpreter,new SenseiIndexReaderDecorator(),new StandardAnalyzer(Version.LUCENE_CURRENT),new DefaultSimilarity(),1000,300000,true);
		return zoieSystem;
	}
	
	public SenseiSearchContext(QueryParser qparser,Map<Integer,IndexReaderFactory<ZoieIndexReader<BoboIndexReader>>> partReaderMap,
							   List<RuntimeFacetHandlerFactory<?>> runtimeFacetHandlerFactories){
		_qparser = qparser;
		_partReaderMap = partReaderMap;
		
		_runtimeFacetHandlerFactories = runtimeFacetHandlerFactories;
	}
	
	public SenseiSearchContext(QueryParser qparser,Map<Integer,File> partFileMap){
		this(qparser,new NoOpIndexableInterpreter(),partFileMap,null);
	}
	
	public SenseiSearchContext(QueryParser qparser,ZoieIndexableInterpreter<?> interpreter,Map<Integer,File> partFileMap){
		this(qparser,interpreter,partFileMap,null);
	}
	
	public SenseiSearchContext(QueryParser qparser,ZoieIndexableInterpreter<?> interpreter,Map<Integer,File> partFileMap,List<RuntimeFacetHandlerFactory<?>> runtimeFacetHandlerFactories){
		_qparser = qparser;
		_runtimeFacetHandlerFactories = runtimeFacetHandlerFactories;
		
		_partReaderMap = new HashMap<Integer,IndexReaderFactory<ZoieIndexReader<BoboIndexReader>>>();
		Set<Entry<Integer,File>> entrySet = partFileMap.entrySet();
		for (Entry<Integer,File> entry : entrySet){
			_partReaderMap.put(entry.getKey(), buildReaderFactory(entry.getValue(), interpreter));
		}
	}

	public QueryParser getQparser() {
		return _qparser;
	}

	public IndexReaderFactory<ZoieIndexReader<BoboIndexReader>> getIdxReaderFactory(int partition) {
		return _partReaderMap == null ? null : _partReaderMap.get(partition);
	}
	
	public Map<Integer,IndexReaderFactory<ZoieIndexReader<BoboIndexReader>>>  getPartitionReaderMap(){
		return _partReaderMap;
	}

	public List<RuntimeFacetHandlerFactory<?>> getRuntimeFacetHandlerFactories() {
		return _runtimeFacetHandlerFactories;
	}
}
