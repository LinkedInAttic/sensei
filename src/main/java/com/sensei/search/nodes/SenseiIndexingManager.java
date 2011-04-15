package com.sensei.search.nodes;

import java.util.Map;

import proj.zoie.api.Zoie;
import proj.zoie.api.ZoieVersion;

import com.browseengine.bobo.api.BoboIndexReader;

public interface SenseiIndexingManager<D,V extends ZoieVersion> {
  void initialize(Map<Integer,Zoie<BoboIndexReader,D,V>> zoieSystemMap) throws Exception;
  void start() throws Exception;
  void shutdown();
}
