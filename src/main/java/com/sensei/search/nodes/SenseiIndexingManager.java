package com.sensei.search.nodes;

import java.util.Map;

import proj.zoie.api.Zoie;

import com.browseengine.bobo.api.BoboIndexReader;

public interface SenseiIndexingManager {
  void initialize(Map<Integer,Zoie<BoboIndexReader,?,?>> zoieSystemMap) throws Exception;
  void start() throws Exception;
  void shutdown();
}
