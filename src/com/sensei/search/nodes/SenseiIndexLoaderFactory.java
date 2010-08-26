package com.sensei.search.nodes;

import com.browseengine.bobo.api.BoboIndexReader;

import proj.zoie.api.Zoie;

public interface SenseiIndexLoaderFactory<V>
{
  SenseiIndexLoader getIndexLoader(int partitionId, Zoie<BoboIndexReader, V> dataConsumer);
}
