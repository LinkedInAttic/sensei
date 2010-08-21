package com.sensei.search.nodes;

import proj.zoie.api.ZoieVersion;
import proj.zoie.impl.indexing.ZoieSystem;

public interface SenseiIndexLoaderFactory<T,V extends ZoieVersion>
{
  SenseiIndexLoader getIndexLoader(int partitionId, ZoieSystem<?,T,V> dataConsumer);
}
