package com.sensei.test;

import java.io.File;

import proj.zoie.api.DefaultZoieVersion;
import proj.zoie.api.indexing.ZoieIndexableInterpreter;
import proj.zoie.impl.indexing.ZoieConfig;
import proj.zoie.impl.indexing.ZoieSystem;

import com.browseengine.bobo.api.BoboIndexReader;
import com.sensei.search.nodes.SenseiIndexReaderDecorator;
import com.sensei.search.nodes.SenseiZoieSystemFactory;

public class TestZoieFactory<T> extends SenseiZoieSystemFactory<T,DefaultZoieVersion> {
	  
	  public TestZoieFactory(File idxDir, ZoieIndexableInterpreter<T> interpreter, SenseiIndexReaderDecorator indexReaderDecorator,
	                               ZoieConfig<DefaultZoieVersion> zoieConfig)
	  {
	    super(idxDir, interpreter, indexReaderDecorator, zoieConfig);
	  }
	  
	  @Override
	  public ZoieSystem<BoboIndexReader,T,DefaultZoieVersion> getZoieInstance(int nodeId,int partitionId)
	  {
		  File partDir = getPath(nodeId,partitionId);
		    if(!partDir.exists())
		    {
		      partDir.mkdirs();
		    }
		    return new ZoieSystem<BoboIndexReader,T,DefaultZoieVersion>(partDir, _interpreter, _indexReaderDecorator, _zoieConfig);
	  }
	  
	  @Override
	  public File getPath(int nodeId,int partitionId)
	  {
	    return _idxDir;
	  }
}
