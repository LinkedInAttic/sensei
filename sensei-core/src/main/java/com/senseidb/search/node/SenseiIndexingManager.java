package com.senseidb.search.node;

import java.util.Map;

import com.linkedin.zoie.api.DataProvider;
import com.linkedin.zoie.api.Zoie;
import com.linkedin.zoie.api.ZoieException;

import com.linkedin.bobo.api.BoboIndexReader;

public interface SenseiIndexingManager<D> {
  void initialize(Map<Integer,Zoie<BoboIndexReader,D>> zoieSystemMap) throws Exception;
  void start() throws Exception;
  void shutdown();
  DataProvider<D> getDataProvider();
  void syncWithVersion(long timeToWait, String version) throws ZoieException;
}
