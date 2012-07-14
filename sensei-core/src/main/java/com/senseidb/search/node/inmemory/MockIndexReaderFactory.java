package com.senseidb.search.node.inmemory;

import java.io.IOException;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.util.Version;

import com.browseengine.bobo.api.BoboIndexReader;

import proj.zoie.api.IndexReaderFactory;
import proj.zoie.api.ZoieIndexReader;

public class MockIndexReaderFactory<T> implements  IndexReaderFactory<ZoieIndexReader<BoboIndexReader> > {
  private Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_35);
  private final ThreadLocal<List<ZoieIndexReader<BoboIndexReader>>> readers = new ThreadLocal<List<ZoieIndexReader<BoboIndexReader>>>();
  
  public MockIndexReaderFactory() {
    
  }
  
  @Override
  public List<ZoieIndexReader<BoboIndexReader>> getIndexReaders() throws IOException {
    return readers.get();
    
  }
  public void setIndexReadersForCurrentThread(List<ZoieIndexReader<BoboIndexReader>> newReaders) {
    readers.set(newReaders); 
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
