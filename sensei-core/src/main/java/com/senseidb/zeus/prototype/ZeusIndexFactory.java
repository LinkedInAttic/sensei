package com.senseidb.zeus.prototype;

import java.io.IOException;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexReader;

import proj.zoie.api.IndexReaderFactory;

public class ZeusIndexFactory implements IndexReaderFactory<IndexReader>{

  @Override
  public List<IndexReader> getIndexReaders() throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Analyzer getAnalyzer() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void returnIndexReaders(List<IndexReader> r) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public String getCurrentReaderVersion() {
    // TODO Auto-generated method stub
    return null;
  }

}
