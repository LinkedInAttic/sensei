package com.sensei.search.nodes;

import java.io.File;

import proj.zoie.api.Zoie;

import com.browseengine.bobo.api.BoboIndexReader;

public abstract class SenseiZoieFactory<V>
{ 
  public static File getPath(File idxDir,int nodeId,int partitionId){
    File nodeLevelFile = new File(idxDir, "node"+nodeId);  
    return new File(nodeLevelFile, "shard"+partitionId); 
  }
  
  public abstract Zoie<BoboIndexReader,V> getZoieInstance(int nodeId,int partitionId);

  // TODO: change to getDirectoryManager
  public abstract File getPath(int nodeId,int partitionId);
}
