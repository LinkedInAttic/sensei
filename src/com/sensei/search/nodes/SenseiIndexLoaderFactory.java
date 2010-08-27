package com.sensei.search.nodes;

import proj.zoie.api.ZoieVersion;
import proj.zoie.api.Zoie;
import com.browseengine.bobo.api.BoboIndexReader;


public interface SenseiIndexLoaderFactory<T,V extends ZoieVersion>
{
  SenseiIndexLoader getIndexLoader(int partitionId, Zoie<BoboIndexReader, T, V> dataConsumer);
}
