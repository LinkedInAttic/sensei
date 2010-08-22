package com.sensei.search.nodes;

import java.io.File;

import org.apache.log4j.Logger;

import proj.zoie.api.ZoieVersion;
import proj.zoie.api.indexing.IndexReaderDecorator;
import proj.zoie.api.indexing.ZoieIndexableInterpreter;
import proj.zoie.impl.indexing.ZoieConfig;
import proj.zoie.impl.indexing.ZoieSystem;

import com.browseengine.bobo.api.BoboIndexReader;

public class SenseiZoieSystemFactory<T,V extends ZoieVersion>
{
  private static Logger log = Logger.getLogger(SenseiZoieSystemFactory.class);
  protected File _idxDir;
  protected ZoieIndexableInterpreter<T> _interpreter;
  protected IndexReaderDecorator<BoboIndexReader> _indexReaderDecorator;
  
  protected final ZoieConfig<V> _zoieConfig;
  
  public SenseiZoieSystemFactory(File idxDir, ZoieIndexableInterpreter<T> interpreter, IndexReaderDecorator<BoboIndexReader> indexReaderDecorator,
                                 ZoieConfig<V> zoieConfig)
  {
    _idxDir = idxDir;
    _interpreter = interpreter;
    _indexReaderDecorator = indexReaderDecorator;
    _zoieConfig = zoieConfig;
  }
  
  public static File getPath(File idxDir,int nodeId,int partitionId){
	  File nodeLevelFile = new File(idxDir, "node"+nodeId);  
	  return new File(nodeLevelFile, "shard"+partitionId); 
  }
  
  public ZoieSystem<BoboIndexReader,T,V> getZoieSystem(int nodeId,int partitionId)
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
  protected File getPath(int nodeId,int partitionId)
  {
    return getPath(_idxDir,nodeId,partitionId);
  }
}
