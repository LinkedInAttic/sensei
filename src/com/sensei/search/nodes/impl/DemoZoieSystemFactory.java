package com.sensei.search.nodes.impl;

import java.io.File;

import proj.zoie.api.indexing.IndexReaderDecorator;
import proj.zoie.api.indexing.ZoieIndexableInterpreter;
import proj.zoie.impl.indexing.ZoieConfig;
import proj.zoie.impl.indexing.ZoieSystem;

import com.browseengine.bobo.api.BoboIndexReader;
import com.sensei.search.nodes.SenseiZoieSystemFactory;

public class DemoZoieSystemFactory<V> extends SenseiZoieSystemFactory<V>
{
  private ZoieSystem<BoboIndexReader,V> _zoieSystem = null;
  
  public DemoZoieSystemFactory(File idxDir, ZoieIndexableInterpreter<V> interpreter, IndexReaderDecorator<BoboIndexReader> indexReaderDecorator,
                               ZoieConfig zoieConfig)
  {
    super(idxDir, interpreter, indexReaderDecorator, zoieConfig);
  }
  
  @Override
  public ZoieSystem<BoboIndexReader,V> getZoieSystem(int partitionId)
  {
    if(_zoieSystem == null)
    {
      _zoieSystem = super.getZoieSystem(partitionId);
    }
    return _zoieSystem;
  }
  
  @Override
  protected File getPath(int partitionId)
  {
    return _idxDir;
  }
}
