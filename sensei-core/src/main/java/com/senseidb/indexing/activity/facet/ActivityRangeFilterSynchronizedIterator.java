package com.senseidb.indexing.activity.facet;

import java.io.IOException;

/**
 * @author vzhabiuk
 * Is used only for testing
 */
public class ActivityRangeFilterSynchronizedIterator extends ActivityRangeFilterIterator {


  public ActivityRangeFilterSynchronizedIterator(int[] fieldValues, int[] indexes, int start, int end) {
    super(fieldValues, indexes, start, end);    
  }
  @Override
  public int nextDoc() throws IOException {
   synchronized (fieldValues) {
     return super.nextDoc();
   }
  }
@Override
public int advance(int id) throws IOException {
  synchronized (fieldValues) {
    return super.advance(id);
  }
}
}
