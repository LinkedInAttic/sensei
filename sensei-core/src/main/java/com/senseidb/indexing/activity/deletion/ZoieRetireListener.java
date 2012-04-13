package com.senseidb.indexing.activity.deletion;

import org.apache.lucene.index.IndexReader;

import proj.zoie.impl.indexing.ZoieSystem;

public interface ZoieRetireListener {
  public void onZoieInstanceRetire(ZoieSystem<IndexReader, ?> zoieSystem); 
}
