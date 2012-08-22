package com.senseidb.zeus.prototype;

import java.io.IOException;

import org.apache.lucene.index.IndexReader;

import proj.zoie.api.ZoieSegmentReader;
import proj.zoie.api.indexing.IndexReaderDecorator;

public class OfflineSegmentAdapter<R extends IndexReader> extends ZoieSegmentReader<R> {

  private final int docLength;
  public OfflineSegmentAdapter(IndexReader in, IndexReaderDecorator<R> decorator, int docLength) throws IOException {
    super(in, decorator);
    this.docLength = docLength;    
  }
  @Override
  public int maxDoc() {   
    return docLength - 1;
  }
@Override
public int numDocs() {
  return maxDoc() + 1;
}
@Override
public long getUID(int docid) {
 
  return 1;
}
@Override
public boolean hasDeletions() {
  return false;
}
@Override
public String getSegmentName() {
  return "Segment" + this.hashCode();
}
}
