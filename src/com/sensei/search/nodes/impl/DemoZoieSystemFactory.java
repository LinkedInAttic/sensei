package com.sensei.search.nodes.impl;

import java.io.File;

import proj.zoie.api.DefaultZoieVersion;
import proj.zoie.api.indexing.IndexReaderDecorator;
import proj.zoie.api.indexing.ZoieIndexableInterpreter;
import proj.zoie.impl.indexing.ZoieConfig;
import proj.zoie.impl.indexing.ZoieSystem;

import com.browseengine.bobo.api.BoboIndexReader;
import com.sensei.search.nodes.SenseiZoieSystemFactory;

public class DemoZoieSystemFactory<T> extends SenseiZoieSystemFactory<T,DefaultZoieVersion>
{
  private ZoieSystem<BoboIndexReader,T,DefaultZoieVersion> _zoieSystem = null;
  
  public DemoZoieSystemFactory(File idxDir, ZoieIndexableInterpreter<T> interpreter, IndexReaderDecorator<BoboIndexReader> indexReaderDecorator,
                               ZoieConfig<DefaultZoieVersion> zoieConfig)
  {
    super(idxDir, interpreter, indexReaderDecorator, zoieConfig);
  }
  
  @Override
  public ZoieSystem<BoboIndexReader,T,DefaultZoieVersion> getZoieSystem(int nodeId,int partitionId)
  {
    if(_zoieSystem == null)
    {
      _zoieSystem = super.getZoieSystem(nodeId,partitionId);
    }
    return _zoieSystem;
  }
  
  @Override
  protected File getPath(int nodeId,int partitionId)
  {
    return _idxDir;
  }
}
