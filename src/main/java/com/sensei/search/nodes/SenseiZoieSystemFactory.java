package com.sensei.search.nodes;

import java.io.File;

import org.apache.log4j.Logger;

import proj.zoie.api.ZoieVersion;
import proj.zoie.api.indexing.ZoieIndexableInterpreter;
import proj.zoie.impl.indexing.ZoieConfig;
import proj.zoie.impl.indexing.ZoieSystem;

import com.browseengine.bobo.api.BoboIndexReader;

public class SenseiZoieSystemFactory<T, V extends ZoieVersion> extends SenseiZoieFactory<T, V>
{
  private static Logger log = Logger.getLogger(SenseiZoieSystemFactory.class);
  
  public SenseiZoieSystemFactory(File idxDir, ZoieIndexableInterpreter<T> interpreter, SenseiIndexReaderDecorator indexReaderDecorator,
                                 ZoieConfig<V> zoieConfig)
  {
    super(idxDir,interpreter,indexReaderDecorator,zoieConfig);
  }
  
  @Override
  public ZoieSystem<BoboIndexReader,T,V> getZoieInstance(int nodeId,int partitionId)
  {
    File partDir = getPath(nodeId,partitionId);
    if(!partDir.exists())
    {
      partDir.mkdirs();
      log.info("nodeId="+nodeId+", partition=" + partitionId + " does not exist, directory created.");
    }
    return new ZoieSystem<BoboIndexReader,T,V>(partDir, _interpreter, _indexReaderDecorator, _zoieConfig);
  }
  
  // TODO: change to getDirectoryManager
  public File getPath(int nodeId,int partitionId)
  {
    return getPath(_idxDir,nodeId,partitionId);
  }
}
