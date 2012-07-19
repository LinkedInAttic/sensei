package com.senseidb.search.node.inmemory;

import java.io.IOException;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.util.Version;

import proj.zoie.api.IndexReaderFactory;
import proj.zoie.api.ZoieIndexReader;

import com.browseengine.bobo.api.BoboIndexReader;

public class MockIndexReaderFactory<T> implements  IndexReaderFactory<ZoieIndexReader<BoboIndexReader> > {
  private Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_35);
 
  private final List<ZoieIndexReader<BoboIndexReader>> readers;
  
  public MockIndexReaderFactory(List<ZoieIndexReader<BoboIndexReader>> readers) {
    this.readers = readers;
   
  }
  
  @Override
  public List<ZoieIndexReader<BoboIndexReader>> getIndexReaders() throws IOException {
    return readers;
    
  }
 
  @Override
  public Analyzer getAnalyzer() {
   
    return analyzer;
  }

  @Override
  public void returnIndexReaders(List<ZoieIndexReader<BoboIndexReader>> r) {
   
  }

  @Override
  public String getCurrentReaderVersion() {
    
    return null;
  }

}
