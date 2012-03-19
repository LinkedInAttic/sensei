package com.senseidb.indexing.activity.time;

import static org.junit.Assert.*;

import java.io.File;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import scala.actors.threadpool.Arrays;

import com.senseidb.test.SenseiStarter;

public class TimeAggregatedActivityValuesTest extends Assert {
  private File dir;
  @Before
  public void before() {
    String pathname = getDirPath();
    SenseiStarter.rmrf(new File("sensei-test"));
    dir = new File(pathname);
    dir.mkdirs();
  }
  
  public static String getDirPath() {
    return "sensei-test/activity-aggregated";
  }
  @After
  public void tearDown() throws Exception {
    File file = new File("sensei-test");    
    SenseiStarter.rmrf(file);
    Clock.setPredefinedTimeInMinutes(0);
  }
  @Test
  public void test1() {
    TimeAggregatedActivityValues timeAggregatedActivityValues = new TimeAggregatedActivityValues("likes", java.util.Arrays.asList("10m","5m", "2m"), 0, getDirPath());
    timeAggregatedActivityValues.init(0);
    for (int i = 0; i < 11; i++) {
      Clock.setPredefinedTimeInMinutes(i);
      timeAggregatedActivityValues.update(0, "1");
      timeAggregatedActivityValues.update(1, "1");
    }
    assertTrue(Arrays.toString( timeAggregatedActivityValues.timeActivities.getActivities(0).array), Arrays.equals(new int[] {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0}, timeAggregatedActivityValues.timeActivities.getActivities(0).array));
    assertTrue(Arrays.toString( timeAggregatedActivityValues.timeActivities.getTimes(0).array), Arrays.equals(new int[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 0, 0, 0, 0, 0, 0, 0, 0, 0}, timeAggregatedActivityValues.timeActivities.getTimes(0).array));   
    assertEquals(timeAggregatedActivityValues.valuesMap.get("10m").fieldValues[0], 11);
    
  }

}
