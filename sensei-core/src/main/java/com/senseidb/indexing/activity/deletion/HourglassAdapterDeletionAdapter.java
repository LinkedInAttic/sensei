package com.senseidb.indexing.activity.deletion;

import java.io.IOException;
import java.util.List;

import org.apache.lucene.index.IndexReader;

import proj.zoie.api.ZoieIndexReader;
import proj.zoie.api.ZoieMultiReader;
import proj.zoie.api.ZoieSegmentReader;
import proj.zoie.impl.indexing.ZoieSystem;

public class HourglassAdapterDeletionAdapter {
  private final DeletionListener deletionListener;

  public HourglassAdapterDeletionAdapter(DeletionListener deletionListener) {
    this.deletionListener = deletionListener;
    // TODO Auto-generated constructor stub
  }
  public void onZoieInstanceRetire(ZoieSystem<IndexReader, ?> zoieSystem)  {
    List<ZoieIndexReader<IndexReader>> indexReaders = null;
    try {
       indexReaders = zoieSystem.getIndexReaders();
       for (ZoieIndexReader indexReader : indexReaders) {
         handleIndexReader(indexReader);
       }
    } catch (IOException e) {
     throw new RuntimeException(e);
    } finally {
      if (indexReaders != null) {
        zoieSystem.returnIndexReaders(indexReaders);
      }
    }
  }

  public void handleIndexReader(ZoieIndexReader indexReader) {
    if (indexReader instanceof ZoieMultiReader) {
      ZoieSegmentReader[] segments = (ZoieSegmentReader[]) ((ZoieMultiReader) indexReader).getSequentialSubReaders();
      for (ZoieSegmentReader segmentReader : segments) {
        handleSegment(segmentReader);
      }
    } else if (indexReader instanceof ZoieSegmentReader) {
      handleSegment((ZoieSegmentReader) indexReader);
    } else {
      throw new UnsupportedOperationException("Only segment and multisegment readers can be handled");
    }
    
  }

  private void handleSegment(ZoieSegmentReader segmentReader) {  
      //if the uid is marked as deleted, that means we have updateable hourglass. In this case the activity 
      //value should not be deleted, as it might be in another Zoie
      long[] uids = segmentReader.getUIDArray();
      if (segmentReader.getDelDocIds() != null)
          for (int docId : segmentReader.getDelDocIds()) {
              if (docId >= 0 && docId < uids.length) {
                  uids[docId] = Long.MIN_VALUE;
              }
          }
      deletionListener.onDelete(segmentReader, uids);      
  }
}
