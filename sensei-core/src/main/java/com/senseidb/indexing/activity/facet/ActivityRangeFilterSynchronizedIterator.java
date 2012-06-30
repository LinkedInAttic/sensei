package com.senseidb.indexing.activity.facet;

import java.io.IOException;

/**
 * @author vzhabiuk
 * Is used only for testing
 */
public class ActivityRangeFilterSynchronizedIterator extends ActivityRangeFilterIterator {


  public ActivityRangeFilterSynchronizedIterator(int[] fieldValues, int[] indexes, int start, int end) {
    super(fieldValues, indexes, start, end);  
   /* synchronized(SynchronizedActivityRangeFacetHandler.GLOBAL_ACTIVITY_TEST_LOCK) {
      StringBuilder builder = new StringBuilder();
      for (int i =0; i < Math.min(fieldValues.length, 40); i++) {
        builder.append("," + fieldValues[i]);
      }
      
      builder.append("-" + fieldValues.toString());
      System.out.println(builder.toString());
    }*/
  }
  @Override
  public int nextDoc() throws IOException {
   synchronized (SynchronizedActivityRangeFacetHandler.GLOBAL_ACTIVITY_TEST_LOCK) {
     return super.nextDoc();
   }
  }
@Override
public int advance(int id) throws IOException {
  synchronized (SynchronizedActivityRangeFacetHandler.GLOBAL_ACTIVITY_TEST_LOCK) {
    return super.advance(id);
  }
}
}
