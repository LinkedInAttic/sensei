package com.sensei.search.nodes;

import java.io.File;
import java.io.FileNotFoundException;

import proj.zoie.api.indexing.IndexReaderDecorator;
import proj.zoie.api.indexing.ZoieIndexableInterpreter;
import proj.zoie.impl.indexing.ZoieConfig;
import proj.zoie.impl.indexing.ZoieSystem;

import com.browseengine.bobo.api.BoboIndexReader;

public class SenseiZoieSystemFactory<V>
{
  protected File _idxDir;
  protected ZoieIndexableInterpreter<V> _interpreter;
  protected IndexReaderDecorator<BoboIndexReader> _indexReaderDecorator;
  
  protected final ZoieConfig _zoieConfig;
  
  public SenseiZoieSystemFactory(File idxDir, ZoieIndexableInterpreter<V> interpreter, IndexReaderDecorator<BoboIndexReader> indexReaderDecorator,
                                 ZoieConfig zoieConfig)
  {
    _idxDir = idxDir;
    _interpreter = interpreter;
    _indexReaderDecorator = indexReaderDecorator;
    _zoieConfig = zoieConfig;
  }
  
  public ZoieSystem<BoboIndexReader,V> getZoieSystem(int partitionId) throws FileNotFoundException
  {
    File partDir = getPath(partitionId);
    if(!partDir.exists())
    {
      throw new FileNotFoundException("partition=" + partitionId + " does not exist");
    }
    return new ZoieSystem<BoboIndexReader,V>(getPath(partitionId), _interpreter, _indexReaderDecorator, _zoieConfig);
  }
  
  // TODO: change to getDirectoryManager
  protected File getPath(int partitionId)
  {
    return new File(_idxDir, "shard"+partitionId);
  }
}
