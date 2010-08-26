package com.sensei.search.nodes.impl;

import proj.zoie.api.Zoie;

import com.browseengine.bobo.api.BoboIndexReader;
import com.sensei.search.nodes.SenseiIndexLoader;
import com.sensei.search.nodes.SenseiIndexLoaderFactory;

public class NoopIndexLoaderFactory<V> implements SenseiIndexLoaderFactory<V>
{
  public SenseiIndexLoader getIndexLoader(int partitionId, Zoie<BoboIndexReader,V> zoieSystem)
  {
    return new NoopIndexLoader<V>(zoieSystem);
  }
}
