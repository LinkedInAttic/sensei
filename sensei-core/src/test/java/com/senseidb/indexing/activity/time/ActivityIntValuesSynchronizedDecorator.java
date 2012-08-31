package com.senseidb.indexing.activity.time;

import com.senseidb.indexing.activity.facet.SynchronizedActivityRangeFacetHandler;
import com.senseidb.indexing.activity.primitives.ActivityIntValues;
import com.senseidb.indexing.activity.time.TimeAggregatedActivityValues;
import com.senseidb.indexing.activity.time.TimeAggregatedActivityValues.IntValueHolder;

public class ActivityIntValuesSynchronizedDecorator extends ActivityIntValues {
  private final ActivityIntValues decorated;

  @Override
  public void init(int capacity) {
    decorated.init(capacity);
  }

  public void init() {
    decorated.init();
  }
  
  @Override
  public boolean update(int index, Object value) {
    synchronized(SynchronizedActivityRangeFacetHandler.GLOBAL_ACTIVITY_TEST_LOCK) {
      //System.out.println("!!!Update" + value);
      return decorated.update(index, value);
    }
  }

 
  public void delete(int index) {
    synchronized(SynchronizedActivityRangeFacetHandler.GLOBAL_ACTIVITY_TEST_LOCK) {
      decorated.delete(index);
    }
  }
  protected ActivityIntValuesSynchronizedDecorator(ActivityIntValues decorated) {
    this.decorated = decorated;
  }
  

 
  public Runnable prepareFlush() {
    return this.decorated.prepareFlush();
  }

  public int getIntValue(int index) {
    synchronized(SynchronizedActivityRangeFacetHandler.GLOBAL_ACTIVITY_TEST_LOCK) {
      return this.decorated.getIntValue(index);
    }
  }

 


  public int[] getFieldValues() {
   return decorated.getFieldValues();
  }

  public void setFieldValues(int[] fieldValues) {
    decorated.setFieldValues(fieldValues);
  }

 
  @Override
  public void close() {
    decorated.close();
  }

  @Override
  public String getFieldName() {
    return decorated.getFieldName();
  }
  public static void decorate( TimeAggregatedActivityValues timeAggregatedActivityValues) {
    timeAggregatedActivityValues.defaultIntValues = new ActivityIntValuesSynchronizedDecorator(timeAggregatedActivityValues.defaultIntValues);
    for (IntValueHolder intValueHolder :  timeAggregatedActivityValues.intActivityValues) {
      intValueHolder.activityIntValues = new ActivityIntValuesSynchronizedDecorator(intValueHolder.activityIntValues);
    }
  }
}
