/**
 * This software is licensed to you under the Apache License, Version 2.0 (the
 * "Apache License").
 *
 * LinkedIn's contributions are made under the Apache License. If you contribute
 * to the Software, the contributions will be deemed to have been made under the
 * Apache License, unless you expressly indicate otherwise. Please do not make any
 * contributions that would be inconsistent with the Apache License.
 *
 * You may obtain a copy of the Apache License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, this software
 * distributed under the Apache License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the Apache
 * License for the specific language governing permissions and limitations for the
 * software governed under the Apache License.
 *
 * © 2012 LinkedIn Corp. All Rights Reserved.  
 */
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
