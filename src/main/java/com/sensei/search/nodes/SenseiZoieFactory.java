package com.sensei.search.nodes;

import java.io.File;

import proj.zoie.api.Zoie;
import proj.zoie.api.ZoieVersion;

import com.browseengine.bobo.api.BoboIndexReader;

public abstract class SenseiZoieFactory<D, V extends ZoieVersion>
{ 
  public static File getPath(File idxDir,int nodeId,int partitionId){
    File nodeLevelFile = new File(idxDir, "node"+nodeId);  
    return new File(nodeLevelFile, "shard"+partitionId); 
  }
  
  public abstract Zoie<BoboIndexReader,D, V> getZoieInstance(int nodeId,int partitionId);

  // TODO: change to getDirectoryManager
  public abstract File getPath(int nodeId,int partitionId);
}
