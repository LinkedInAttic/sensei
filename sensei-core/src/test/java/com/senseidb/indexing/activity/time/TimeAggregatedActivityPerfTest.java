package com.senseidb.indexing.activity.time;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import proj.zoie.impl.indexing.ZoieConfig;

import com.senseidb.indexing.activity.ActivityPersistenceFactory;
import com.senseidb.indexing.activity.CompositeActivityManager;
import com.senseidb.indexing.activity.CompositeActivityValues;
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
  
 @Ignore
  @Test
  public void test1Perf10mInsertsAndUpdateAfterwards() throws Exception {
    Clock.setPredefinedTimeInMinutes(0);
    CompositeActivityValues activityValues = CompositeActivityValues.createCompositeValues(ActivityPersistenceFactory.getInstance(getDirPath()), Collections.EMPTY_LIST, Arrays.asList(new CompositeActivityManager.TimeAggregateInfo("likes", Arrays.asList("10m","5m", "2m"))) , ZoieConfig.DEFAULT_VERSION_COMPARATOR);
    TimeAggregatedActivityValues timeAggregatedActivityValues = (TimeAggregatedActivityValues) activityValues.getActivityValuesMap().get("likes");
    timeAggregatedActivityValues.getAggregatesUpdateJob().stop();
    long insertTime = System.currentTimeMillis();
    Map<String, Object> jsonActivityUpdate = new HashMap<String, Object>();
    jsonActivityUpdate.put("likes", "+1");
    int recordsCount = 1000000;
    int numOfEvents = 10;
    for (int i = 0; i < numOfEvents; i++) {
      Clock.setPredefinedTimeInMinutes(i);
      for (int j = 0; j < recordsCount; j ++) {
        activityValues.update((long)j, String.valueOf(i * recordsCount + j), jsonActivityUpdate);
        if (j % 100000 == 0) {
          System.out.println("Inserted next 100k events j = " + j);
        }
      }
      System.out.println("Updated event = " + i);
    }
    System.out.println("insertTime = " + (System.currentTimeMillis() - insertTime));
    activityValues.syncWithPersistentVersion(String.valueOf((numOfEvents - 1) * recordsCount + recordsCount - 1));
    System.out.println("persistentTime = " + (System.currentTimeMillis() - insertTime));
    assertEquals(numOfEvents, timeAggregatedActivityValues.getValuesMap().get("5m").fieldValues[50000]);
    assertEquals(numOfEvents, timeAggregatedActivityValues.getValuesMap().get("10m").fieldValues[50000]);
    assertEquals(numOfEvents, timeAggregatedActivityValues.getValuesMap().get("10m").fieldValues[90000]);
    long updateTime = System.currentTimeMillis();
    timeAggregatedActivityValues.getAggregatesUpdateJob().run();
    assertEquals(5, timeAggregatedActivityValues.getValuesMap().get("5m").fieldValues[50000]);
    assertEquals(10, timeAggregatedActivityValues.getValuesMap().get("10m").fieldValues[50000]);
    assertEquals(10, timeAggregatedActivityValues.getValuesMap().get("10m").fieldValues[90000]);
    System.out.println("updateTime = " + (System.currentTimeMillis() - updateTime)); 
    activityValues.flush();
    Thread.sleep(2000);
    activityValues.close();
    
     activityValues = CompositeActivityValues.createCompositeValues(ActivityPersistenceFactory.getInstance(getDirPath()), Collections.EMPTY_LIST, Arrays.asList(new CompositeActivityManager.TimeAggregateInfo("likes", Arrays.asList("10m","5m", "2m"))) , ZoieConfig.DEFAULT_VERSION_COMPARATOR);
    timeAggregatedActivityValues = (TimeAggregatedActivityValues) activityValues.getActivityValuesMap().get("likes");
    timeAggregatedActivityValues.getAggregatesUpdateJob().stop();
    assertEquals(5, timeAggregatedActivityValues.getValuesMap().get("5m").fieldValues[50000]);
    assertEquals(10, timeAggregatedActivityValues.getValuesMap().get("10m").fieldValues[50000]);
    assertEquals(10, timeAggregatedActivityValues.getValuesMap().get("10m").fieldValues[90000]);
  }

}
