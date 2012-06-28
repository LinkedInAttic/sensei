package com.senseidb.indexing.activity.time;

import static org.junit.Assert.*;

import java.io.File;

import junit.framework.Assert;
import junit.framework.TestCase;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import scala.actors.threadpool.Arrays;

import com.senseidb.indexing.activity.time.TimeAggregatedActivityValues.AggregatesMetadata;
import com.senseidb.test.SenseiStarter;

public class TimeAggregatedActivityValuesTest extends TestCase {
  private File dir;
  private TimeAggregatedActivityValues timeAggregatedActivityValues;

  public void setUp() {
    String pathname = getDirPath();
    SenseiStarter.rmrf(new File("sensei-test"));
    dir = new File(pathname);
    dir.mkdirs();
  }
  
  public static String getDirPath() {
    return "sensei-test/activity-aggregated";
  }
 
  public void tearDown() throws Exception {
    File file = new File("sensei-test");    
    SenseiStarter.rmrf(file);
    Clock.setPredefinedTimeInMinutes(0);
  }
 
  public void test1() {
    Clock.setPredefinedTimeInMinutes(0);
     timeAggregatedActivityValues = new TimeAggregatedActivityValues("likes", java.util.Arrays.asList("10m","5m", "2m"), 0, getDirPath());
    timeAggregatedActivityValues.init(0);
    for (int i = 0; i < 11; i++) {
      Clock.setPredefinedTimeInMinutes(i);
      timeAggregatedActivityValues.update(0, "1");
      timeAggregatedActivityValues.update(1, "1");
    }
    assertTrue(Arrays.toString( timeAggregatedActivityValues.timeActivities.getActivities(0).array), Arrays.equals(new int[] {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}, timeAggregatedActivityValues.timeActivities.getActivities(0).array));
    assertTrue(Arrays.toString( timeAggregatedActivityValues.timeActivities.getTimes(0).array), Arrays.equals(new int[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, timeAggregatedActivityValues.timeActivities.getTimes(0).array));   
    assertEquals(timeAggregatedActivityValues.valuesMap.get("10m").fieldValues[0], 11);
    Clock.setPredefinedTimeInMinutes(10);
    AggregatesUpdateJob aggregatesUpdateJob = new AggregatesUpdateJob(timeAggregatedActivityValues, AggregatesMetadata.init(getDirPath(), "likes"));
    Clock.setPredefinedTimeInMinutes(11);
    aggregatesUpdateJob.run();
    assertEquals(timeAggregatedActivityValues.valuesMap.get("10m").fieldValues[0], 9);
    assertEquals(timeAggregatedActivityValues.valuesMap.get("5m").fieldValues[0], 4);
    assertEquals(timeAggregatedActivityValues.valuesMap.get("2m").fieldValues[0], 1);
    assertEquals(timeAggregatedActivityValues.timeActivities.getTimes(0).getSize(), 9);
    Clock.setPredefinedTimeInMinutes(12);
    aggregatesUpdateJob.run();
    assertEquals(timeAggregatedActivityValues.valuesMap.get("10m").fieldValues[0], 8);
    assertEquals(timeAggregatedActivityValues.valuesMap.get("5m").fieldValues[0], 3);
    assertEquals(timeAggregatedActivityValues.valuesMap.get("2m").fieldValues[0], 0);
    assertEquals(timeAggregatedActivityValues.timeActivities.getTimes(0).getSize(), 8);
    Clock.setPredefinedTimeInMinutes(25);
    aggregatesUpdateJob.run();
    assertEquals(timeAggregatedActivityValues.valuesMap.get("10m").fieldValues[0], 0);
    assertEquals(timeAggregatedActivityValues.valuesMap.get("5m").fieldValues[0], 0);
    assertEquals(timeAggregatedActivityValues.valuesMap.get("2m").fieldValues[0], 0);
    
  }

}
