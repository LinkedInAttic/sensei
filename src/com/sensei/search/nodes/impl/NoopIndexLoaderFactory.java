package com.sensei.search.nodes.impl;

import proj.zoie.impl.indexing.ZoieSystem;

import com.sensei.search.nodes.SenseiIndexLoader;
import com.sensei.search.nodes.SenseiIndexLoaderFactory;

public class NoopIndexLoaderFactory<V> implements SenseiIndexLoaderFactory<V>
{
  public SenseiIndexLoader getIndexLoader(int partitionId, ZoieSystem<?,V> zoieSystem)
  {
    return new NoopIndexLoader<V>(zoieSystem);
  }
}
