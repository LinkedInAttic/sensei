package com.sensei.search.nodes;

import proj.zoie.impl.indexing.ZoieSystem;

public interface SenseiIndexLoaderFactory<V>
{
  SenseiIndexLoader getIndexLoader(int partitionId, ZoieSystem<?,V> dataConsumer);
}
