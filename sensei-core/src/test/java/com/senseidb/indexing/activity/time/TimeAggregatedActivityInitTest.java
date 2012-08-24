package com.senseidb.indexing.activity.time;

import static org.junit.Assert.*;

import org.junit.Test;

import scala.actors.threadpool.Arrays;

import com.senseidb.indexing.activity.primitives.ActivityIntValues;
import com.senseidb.indexing.activity.time.TimeAggregatedActivityValues.IntValueHolder;
import com.senseidb.indexing.activity.time.TimeAggregatedActivityValues.TimeHitsHolder;

public class TimeAggregatedActivityInitTest {

  @Test
  public void test1MinutesMoreThanActivities() {
    int capacity = 1;
    TimeHitsHolder timeHitsHolder = new TimeHitsHolder(capacity);
    IntValueHolder[] intValueHolders = new IntValueHolder[3];
    intValueHolders[0] = new IntValueHolder(new ActivityIntValues(capacity), "10m", 10);
    intValueHolders[1] = new IntValueHolder(new ActivityIntValues(capacity), "5m", 5);
    intValueHolders[2] = new IntValueHolder(new ActivityIntValues(capacity), "2m", 2);
    intValueHolders[0].activityIntValues.fieldValues[0] = 5;
    intValueHolders[1].activityIntValues.fieldValues[0] = 3;
    intValueHolders[2].activityIntValues.fieldValues[0] = 1;
    TimeAggregatedActivityValues.initTimeHits(timeHitsHolder, intValueHolders, 1, 10);
    assertTrue(Arrays.equals(new int[] {1, 1, 1, 1, 1, 0, 0, 0, 0}, timeHitsHolder.getActivities(0).array));
    assertTrue(Arrays.equals(new int[] {1, 3, 6, 7, 10, 0, 0, 0, 0}, timeHitsHolder.getTimes(0).array));   
  }
  @Test
  public void test2ActivitiesMoreThanMinutes() {
    int capacity = 1;
    TimeHitsHolder timeHitsHolder = new TimeHitsHolder(capacity);
    IntValueHolder[] intValueHolders = new IntValueHolder[3];
    intValueHolders[0] = new IntValueHolder(new ActivityIntValues(capacity), "10m", 10);
    intValueHolders[1] = new IntValueHolder(new ActivityIntValues(capacity), "5m", 5);
    intValueHolders[2] = new IntValueHolder(new ActivityIntValues(capacity), "2m", 2);
    intValueHolders[0].activityIntValues.fieldValues[0] = 50;
    intValueHolders[1].activityIntValues.fieldValues[0] = 10;
    intValueHolders[2].activityIntValues.fieldValues[0] = 1;
    TimeAggregatedActivityValues.initTimeHits(timeHitsHolder, intValueHolders, 1, 10);
    assertTrue(Arrays.equals(new int[] {8, 8, 8, 8, 8, 3, 3, 3, 1, 0}, timeHitsHolder.getActivities(0).array));
    assertTrue(Arrays.equals(new int[] {1, 2, 3, 4, 5, 6, 7, 8, 10, 0}, timeHitsHolder.getTimes(0).array));   
  }
  @Test
  public void test3SingleActivities() {
    int capacity = 1;
    TimeHitsHolder timeHitsHolder = new TimeHitsHolder(capacity);
    IntValueHolder[] intValueHolders = new IntValueHolder[3];
    intValueHolders[0] = new IntValueHolder(new ActivityIntValues(capacity), "10m", 1);
    intValueHolders[1] = new IntValueHolder(new ActivityIntValues(capacity), "5m", 1);
    intValueHolders[2] = new IntValueHolder(new ActivityIntValues(capacity), "2m", 1);
    intValueHolders[0].activityIntValues.fieldValues[0] = 50;
    intValueHolders[1].activityIntValues.fieldValues[0] = 10;
    intValueHolders[2].activityIntValues.fieldValues[0] = 1;
    TimeAggregatedActivityValues.initTimeHits(timeHitsHolder, intValueHolders, 1, 10);    
    assertTrue(Arrays.toString( timeHitsHolder.getActivities(0).array), Arrays.equals(new int[] {1}, timeHitsHolder.getActivities(0).array));
    assertTrue(Arrays.toString( timeHitsHolder.getTimes(0).array), Arrays.equals(new int[] {10}, timeHitsHolder.getTimes(0).array));   
  }
}
