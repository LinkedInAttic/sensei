package com.senseidb.indexing.activity.time;

import java.io.File;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.senseidb.test.SenseiStarter;

public class TimeAggregatedActivityPerfTest extends Assert {
  private File dir;
  @Before
  public void before() {
    String pathname = getDirPath();
    SenseiStarter.rmrf(new File("sensei-test"));
    dir = new File(pathname);
    dir.mkdirs();
  }
  
  public static String getDirPath() {
    return "sensei-test/activity-aggregated-perf";
  }
  @After
  public void tearDown() throws Exception {
    File file = new File("sensei-test");    
    SenseiStarter.rmrf(file);
    Clock.setPredefinedTimeInMinutes(0);
  }
  @Test
  public void test1Perf10mInsertsAndUpdateAfterwards() {
    Clock.setPredefinedTimeInMinutes(0);
    TimeAggregatedActivityValues timeAggregatedActivityValues = new TimeAggregatedActivityValues("likes", java.util.Arrays.asList("10m","5m", "2m"), 0, getDirPath());
    
    timeAggregatedActivityValues.init(0);
    timeAggregatedActivityValues.getAggregatesUpdateJob().stop();
    long insertTime = System.currentTimeMillis();
    for (int i = 0; i < 100; i++) {
      Clock.setPredefinedTimeInMinutes(i);
      for (int j = 0; j < 100000; j ++) {
        if (timeAggregatedActivityValues.update(j, "+1")) {
          timeAggregatedActivityValues.flush();
        }
      }
      System.out.println("Updated event = " + i);
    }
    System.out.println("insertTime = " + (System.currentTimeMillis() - insertTime));
    assertEquals(100, timeAggregatedActivityValues.getValuesMap().get("5m").fieldValues[50000]);
    assertEquals(100, timeAggregatedActivityValues.getValuesMap().get("10m").fieldValues[50000]);
    assertEquals(100, timeAggregatedActivityValues.getValuesMap().get("10m").fieldValues[90000]);
    long updateTime = System.currentTimeMillis();
    timeAggregatedActivityValues.getAggregatesUpdateJob().run();
    assertEquals(5, timeAggregatedActivityValues.getValuesMap().get("5m").fieldValues[50000]);
    assertEquals(10, timeAggregatedActivityValues.getValuesMap().get("10m").fieldValues[50000]);
    assertEquals(10, timeAggregatedActivityValues.getValuesMap().get("10m").fieldValues[90000]);
    System.out.println("updateTime = " + (System.currentTimeMillis() - updateTime));  
  }

}
