package com.sensei.search.nodes.impl;

import proj.zoie.api.ZoieVersion;
import proj.zoie.impl.indexing.ZoieSystem;

import com.sensei.search.nodes.SenseiIndexLoader;
import com.sensei.search.nodes.SenseiIndexLoaderFactory;

public class NoopIndexLoaderFactory<T,V extends ZoieVersion> implements SenseiIndexLoaderFactory<T,V>
{
  public SenseiIndexLoader getIndexLoader(int partitionId, ZoieSystem<?,T,V> zoieSystem)
  {
    return new NoopIndexLoader(zoieSystem);
  }
}
