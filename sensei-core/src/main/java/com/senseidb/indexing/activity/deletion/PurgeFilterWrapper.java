package com.senseidb.indexing.activity.deletion;

import java.io.IOException;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Filter;

import proj.zoie.api.ZoieIndexReader;

/**
 * This is used to notify the activity engine if the document gets deleted from Zoie by executing the purge filter
 * @author vzhabiuk
 *
 */
public class PurgeFilterWrapper extends Filter {  
  private final Filter internal;
  private final DeletionListener deletionListener;

  public PurgeFilterWrapper(Filter internal, DeletionListener deletionListener) {
    this.internal = internal;
    this.deletionListener = deletionListener;   
  }
  
  @Override
  public DocIdSet getDocIdSet(final IndexReader reader) throws IOException {    
    final ZoieIndexReader zoieIndexReader = (ZoieIndexReader)reader; 
    return new DocIdSet() {
      public DocIdSetIterator iterator() throws IOException {
        return new DocIdSetIteratorWrapper(internal.getDocIdSet(reader).iterator()) {          
          @Override
          protected void handeDoc(int ret) {            
            deletionListener.onDelete(reader, zoieIndexReader.getUID(ret));            
          }
        };
      }
    };
  }

  public abstract static class DocIdSetIteratorWrapper extends DocIdSetIterator {
  private final DocIdSetIterator iterator;
  public DocIdSetIteratorWrapper(DocIdSetIterator iterator) {
    this.iterator = iterator;
  }
    @Override
    public int docID() {
      return iterator.docID();
    }

    @Override
    public int nextDoc() throws IOException {
      int ret = iterator.nextDoc();
      if (ret != DocIdSetIterator.NO_MORE_DOCS) {
        handeDoc(ret);
      }
      return ret;
    }

    @Override
    public int advance(int target) throws IOException {
      int ret = iterator.advance(target);
      if (ret != DocIdSetIterator.NO_MORE_DOCS) {
        handeDoc(ret);
      }
      return ret;
    }
    protected abstract void handeDoc(int ret);    
  }
  
}
