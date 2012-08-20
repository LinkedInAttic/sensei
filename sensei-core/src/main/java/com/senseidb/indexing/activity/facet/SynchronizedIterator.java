package com.senseidb.indexing.activity.facet;

import java.io.IOException;

import org.apache.lucene.search.DocIdSetIterator;

/**
 * @author vzhabiuk
 * Is used only for testing
 */
public class SynchronizedIterator extends DocIdSetIterator {


  private final DocIdSetIterator inner;
  public SynchronizedIterator(DocIdSetIterator inner) {
    this.inner = inner;
  }
  @Override
  public int nextDoc() throws IOException {
   synchronized (SynchronizedActivityRangeFacetHandler.GLOBAL_ACTIVITY_TEST_LOCK) {
     return inner.nextDoc();
   }
  }
@Override
public int advance(int id) throws IOException {
  synchronized (SynchronizedActivityRangeFacetHandler.GLOBAL_ACTIVITY_TEST_LOCK) {
    return inner.advance(id);
  }
}
@Override
public int docID() {
  // TODO Auto-generated method stub
  return inner.docID();
}
}
