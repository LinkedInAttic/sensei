package com.sensei.search.nodes.impl;

import proj.zoie.impl.indexing.ZoieSystem;

import com.sensei.search.nodes.SenseiIndexLoader;
import com.sensei.search.nodes.SenseiIndexLoaderFactory;

public class NoopIndexLoaderFactory implements SenseiIndexLoaderFactory
{
  public SenseiIndexLoader getIndexLoader(int partitionId, ZoieSystem<?,?> zoieSystem)
  {
    return new NoopIndexLoader(zoieSystem);
  }
}
