package com.sensei.search.nodes;

import proj.zoie.impl.indexing.ZoieSystem;

public interface SenseiIndexLoaderFactory
{
  SenseiIndexLoader getIndexLoader(int partitionId, ZoieSystem<?,?> zoieSystem);
}
