package com.sensei.search.nodes;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.search.DefaultSimilarity;
import org.apache.lucene.util.Version;

import proj.zoie.api.DefaultZoieVersion;
import proj.zoie.api.DefaultZoieVersion.DefaultZoieVersionFactory;
import proj.zoie.api.IndexReaderFactory;
import proj.zoie.api.ZoieIndexReader;
import proj.zoie.api.ZoieVersion;
import proj.zoie.api.ZoieVersionFactory;
import proj.zoie.api.indexing.ZoieIndexableInterpreter;
import proj.zoie.impl.indexing.ZoieSystem;

import com.browseengine.bobo.api.BoboIndexReader;

public class SenseiSearchContext {
	private final Map<Integer,SenseiQueryBuilderFactory> _builderFactoryMap;
	private final Map<Integer,IndexReaderFactory<ZoieIndexReader<BoboIndexReader>>> _partReaderMap;

	private static <T,V extends ZoieVersion> IndexReaderFactory<ZoieIndexReader<BoboIndexReader>> buildReaderFactory(File file,ZoieIndexableInterpreter<T> interpreter,ZoieVersionFactory<V> versionFactory){
		ZoieSystem<BoboIndexReader,T,V> zoieSystem = new ZoieSystem<BoboIndexReader,T,V>(file,interpreter,new SenseiIndexReaderDecorator(),new StandardAnalyzer(Version.LUCENE_CURRENT),new DefaultSimilarity(),1000,300000,true,versionFactory);
		zoieSystem.getAdminMBean().setFreshness(50);
		zoieSystem.start();
		return zoieSystem;
	}

	private static <T> IndexReaderFactory<ZoieIndexReader<BoboIndexReader>> buildReaderFactory(File file,ZoieIndexableInterpreter<T> interpreter){
		ZoieSystem<BoboIndexReader,T,DefaultZoieVersion> zoieSystem = new ZoieSystem<BoboIndexReader,T,DefaultZoieVersion>(file,interpreter,new SenseiIndexReaderDecorator(),new StandardAnalyzer(Version.LUCENE_CURRENT),new DefaultSimilarity(),1000,300000,true,new DefaultZoieVersionFactory());
    zoieSystem.getAdminMBean().setFreshness(50);
    zoieSystem.start();
		return zoieSystem;
	}

	public SenseiSearchContext(Map<Integer,SenseiQueryBuilderFactory> builderFactoryMap,Map<Integer,IndexReaderFactory<ZoieIndexReader<BoboIndexReader>>> partReaderMap){
		_builderFactoryMap = builderFactoryMap;
		_partReaderMap = partReaderMap;
	}

	public SenseiSearchContext(Map<Integer,SenseiQueryBuilderFactory> builderFactoryMap,ZoieIndexableInterpreter<?> interpreter,Map<Integer,File> partFileMap){
	  _builderFactoryMap = builderFactoryMap;
		
		_partReaderMap = new HashMap<Integer,IndexReaderFactory<ZoieIndexReader<BoboIndexReader>>>();
		Set<Entry<Integer,File>> entrySet = partFileMap.entrySet();
		for (Entry<Integer,File> entry : entrySet){
			_partReaderMap.put(entry.getKey(), buildReaderFactory(entry.getValue(), interpreter));
		}
	}

	public Map<Integer,SenseiQueryBuilderFactory> getQueryBuilderFactoryMap() {
		return _builderFactoryMap;
	}

	public IndexReaderFactory<ZoieIndexReader<BoboIndexReader>> getIdxReaderFactory(int partition) {
		return _partReaderMap == null ? null : _partReaderMap.get(partition);
	}

	public Map<Integer,IndexReaderFactory<ZoieIndexReader<BoboIndexReader>>>  getPartitionReaderMap(){
		return _partReaderMap;
	}
}
