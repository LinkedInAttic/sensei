package com.sensei.search.nodes;

import java.util.Map;

import proj.zoie.api.DataProvider;
import proj.zoie.api.ZoieException;
import proj.zoie.impl.indexing.AsyncDataConsumer;

public interface SenseiIndexingManager<D> {
  void initialize(Map<Integer,AsyncDataConsumer<D>> dataConsumerMap) throws Exception;
  void start() throws Exception;
  void shutdown();
  DataProvider<D> getDataProvider();
  void syncWithVersion(long timeToWait, String version) throws ZoieException;
}
