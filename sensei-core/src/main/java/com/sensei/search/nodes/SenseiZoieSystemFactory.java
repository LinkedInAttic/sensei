package com.sensei.search.nodes;

import java.io.File;

import org.apache.log4j.Logger;
import org.apache.lucene.search.Filter;

import proj.zoie.api.indexing.ZoieIndexableInterpreter;
import proj.zoie.impl.indexing.ZoieConfig;
import proj.zoie.impl.indexing.ZoieSystem;

import com.browseengine.bobo.api.BoboIndexReader;

public class SenseiZoieSystemFactory<T> extends SenseiZoieFactory<T>
{
  private static Logger log = Logger.getLogger(SenseiZoieSystemFactory.class);
  private Filter _purgeFilter = null;
  
  public SenseiZoieSystemFactory(File idxDir, ZoieIndexableInterpreter<T> interpreter, SenseiIndexReaderDecorator indexReaderDecorator,
                                 ZoieConfig zoieConfig)
  {
    super(idxDir,interpreter,indexReaderDecorator,zoieConfig);
  }
  
  public void setPurgeFilter(Filter purgeFilter){
    _purgeFilter = purgeFilter;
  }
  
  @Override
  public ZoieSystem<BoboIndexReader,T> getZoieInstance(int nodeId,int partitionId)
  {
    File partDir = getPath(nodeId,partitionId);
    if(!partDir.exists())
    {
      partDir.mkdirs();
      log.info("nodeId="+nodeId+", partition=" + partitionId + " does not exist, directory created.");
    }
    ZoieSystem<BoboIndexReader,T> zoie = new ZoieSystem<BoboIndexReader,T>(partDir, _interpreter, _indexReaderDecorator, _zoieConfig);
    if (_purgeFilter!=null){
      zoie.setPurgeFilter(_purgeFilter);
    }
    return zoie;
  }
  
  // TODO: change to getDirectoryManager
  public File getPath(int nodeId,int partitionId)
  {
    return getPath(_idxDir,nodeId,partitionId);
  }
}
