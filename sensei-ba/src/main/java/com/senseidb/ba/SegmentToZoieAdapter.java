package com.senseidb.ba;

import java.io.IOException;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;


import proj.zoie.api.ZoieSegmentReader;
import proj.zoie.api.indexing.IndexReaderDecorator;

public class SegmentToZoieAdapter<R extends IndexReader> extends ZoieSegmentReader<R> {

  private final IndexSegment offlineSegment;

  public SegmentToZoieAdapter(IndexSegment offlineSegment, IndexReaderDecorator<R> decorator) throws IOException {
    super(fakeIndexReader(), null);
    this.offlineSegment = offlineSegment;
    R decorated = decorator.decorate(this);
    try {
    java.lang.reflect.Field decReaderField = ZoieSegmentReader.class.getDeclaredField("_decoratedReader");
    decReaderField.setAccessible(true);
    decReaderField.set(this, decorated);
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  public static IndexReader fakeIndexReader() {
    RAMDirectory directory = new RAMDirectory();
    try {
    IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig(Version.LUCENE_35, new StandardAnalyzer(Version.LUCENE_35)));
    Document doc = new Document();
    doc.add(new Field("fake", "".getBytes()));
    writer.addDocument(doc);
    writer.close();
    
      return IndexReader.open(directory).getSequentialSubReaders()[0];
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public int maxDoc() {
    return offlineSegment.getLength() - 1;
  }

  @Override
  public int numDocs() {
    return offlineSegment.getLength();
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

  public IndexSegment getOfflineSegment() {
    return offlineSegment;
  }
  
}
