package com.sensei.search.nodes;

import org.apache.lucene.index.IndexReader;

import proj.zoie.impl.indexing.ZoieSystem;

public interface SenseiIndexLoaderFactory
{
  SenseiIndexLoader getIndexLoader(int partitionId, ZoieSystem<?,?> zoieSystem);
}
