package com.senseidb.indexing.activity;

import java.util.Collections;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

import junit.framework.TestCase;

import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Ignore;

import proj.zoie.impl.indexing.ZoieConfig;

import com.senseidb.gateway.file.FileDataProviderWithMocks;
import com.senseidb.indexing.activity.time.Clock;
import com.senseidb.indexing.activity.time.TimeAggregatedActivityValues;
import com.senseidb.test.SenseiStarter;
import com.senseidb.test.TestSensei;

public class ActivityIntegrationTest extends TestCase {

  private static final Logger logger = Logger.getLogger(ActivityIntegrationTest.class);
 
  static {
    SenseiStarter.start("test-conf/node1", "test-conf/node2");  
  }
  
  public void test1SendUpdatesAndSort() throws Exception {
    for (int i = 0; i < 10; i++) {
      FileDataProviderWithMocks.add(new JSONObject().put("id", 10L + i).put("_type", "update").put("likes", "+" + (10 + i)));
    }
    final CompositeActivityValues inMemoryColumnData1 = CompositeActivityManager.cachedInstances.get(1).activityValues;
    final CompositeActivityValues inMemoryColumnData2 = CompositeActivityManager.cachedInstances.get(2).activityValues;
    Wait.until(10000, "The activity value wasn't updated", new Wait.Condition() {
      public boolean evaluate() {
        return inMemoryColumnData1.getVersion().equals(String.valueOf(15009)) || inMemoryColumnData2.getVersion().equals(String.valueOf(15009));
      }
    }); 
    String req = "{\"selections\": [{\"range\": {\"likes\": {\"from\": 18, \"include_lower\": true}}}], \"sort\":[{\"likes\":\"desc\"}]}";
    JSONObject res = TestSensei.search(new JSONObject(req));
    JSONArray hits = res.getJSONArray("hits");
    assertEquals(Integer.parseInt(hits.getJSONObject(0).getJSONArray("likes").getString(0)), 20);
    assertEquals(Integer.parseInt(hits.getJSONObject(1).getJSONArray("likes").getString(0)), 19);
    assertEquals(Integer.parseInt(hits.getJSONObject(2).getJSONArray("likes").getString(0)), 18);
   System.out.println("!!!" + res.toString(1));
  }
  
  public void test2SendUpdateAndCheckIfItsPersisted() throws Exception {
    for (int i = 0; i < 5; i++) {
      FileDataProviderWithMocks.add(new JSONObject().put("id", 1L).put("_type", "update").put("likes", "+5").put("color", "blue"));
    }
    final CompositeActivityValues inMemoryColumnData1 = CompositeActivityManager.cachedInstances.get(1).activityValues;
    final CompositeActivityValues inMemoryColumnData2 = CompositeActivityManager.cachedInstances.get(2).activityValues;
    Wait.until(10000, "The activity value wasn't updated", new Wait.Condition() {
      public boolean evaluate() {
        return inMemoryColumnData1.getValueByUID(1L, "likes") == 26 || inMemoryColumnData2.getValueByUID(1L, "likes") == 26;
      }
    });
    for (int i = 0; i < 5; i++) {
      FileDataProviderWithMocks.add(new JSONObject().put("id", 1L).put("_type", "update").put("likes", "+5"));
    }
    Wait.until(10000, "The activity value wasn't updated", new Wait.Condition() {
      public boolean evaluate() {
        return inMemoryColumnData1.getValueByUID(1L, "likes") == 51 || inMemoryColumnData2.getValueByUID(1L, "likes") == 51;
      }
    });
  }
  public void test3AggregatesIntegrationTest() throws Exception {
    final CompositeActivityValues inMemoryColumnData1 = CompositeActivityManager.cachedInstances.get(1).activityValues;
    final CompositeActivityValues inMemoryColumnData2 = CompositeActivityManager.cachedInstances.get(2).activityValues;
    final TimeAggregatedActivityValues timeAggregatedActivityValues1 = clear(inMemoryColumnData1);
    final TimeAggregatedActivityValues timeAggregatedActivityValues2 = clear(inMemoryColumnData2);
    for (ActivityIntValues activityIntValues : timeAggregatedActivityValues1.getValuesMap().values()) {
      assertEquals(0, activityIntValues.getValue(0));
    }
    for (ActivityIntValues activityIntValues : timeAggregatedActivityValues2.getValuesMap().values()) {
      for (int i = 0; i < activityIntValues.fieldValues.length; i ++) {
        assertEquals("" + i, 0, activityIntValues.fieldValues[i]);
      }
    }  
    int initialTime = Clock.getCurrentTimeInMinutes();
    for (int i = 0; i < 10; i++) {
      final int uid = i;
      Clock.setPredefinedTimeInMinutes(Clock.getCurrentTimeInMinutes() + 1);
      for (int j = 0; j < 10 - i; j ++) {
        FileDataProviderWithMocks.add(new JSONObject().put("id", j).put("_type", "update").put("likes", "+1"));
      }
      if (uid < 9)Wait.until(10000, "" + i, new Wait.Condition() {
        @Override
        public boolean evaluate() { 
          return inMemoryColumnData1.getValueByUID(1, "likes") == uid + 1;
        }
      });   
    }
    Wait.until(100000, "", new Wait.Condition() {
      @Override
      public boolean evaluate() { 
        return inMemoryColumnData1.getValueByUID(0, "likes") == 10;
      }
    });   
    String req = "{\"selections\": [{\"range\": {\"likes:2w\": {\"from\": 8, \"include_lower\": true}}}], \"sort\":[{\"likes:2w\":\"desc\"}]}";
    JSONObject res = TestSensei.search(new JSONObject(req));     
    System.out.println("!!!" + res.toString(1));
    JSONArray hits = res.getJSONArray("hits");
    assertEquals(Integer.parseInt(hits.getJSONObject(0).getJSONArray("likes:2w").getString(0)), 10);
    assertEquals(Integer.parseInt(hits.getJSONObject(1).getJSONArray("likes:2w").getString(0)), 9);
    assertEquals(Integer.parseInt(hits.getJSONObject(2).getJSONArray("likes:2w").getString(0)), 8);
    Clock.setPredefinedTimeInMinutes(initialTime + 11);
    timeAggregatedActivityValues1.getAggregatesUpdateJob().run();
    timeAggregatedActivityValues2.getAggregatesUpdateJob().run();
    req = "{ \"sort\":[{\"likes:5m\":\"desc\"}]}";
     res = TestSensei.search(new JSONObject(req));
     hits = res.getJSONArray("hits");
    assertEquals(Integer.parseInt(hits.getJSONObject(0).getJSONArray("likes:5m").getString(0)), 4);
    assertEquals(Integer.parseInt(hits.getJSONObject(1).getJSONArray("likes:5m").getString(0)), 3);
    assertEquals(Integer.parseInt(hits.getJSONObject(2).getJSONArray("likes:5m").getString(0)), 2);
    assertEquals(Integer.parseInt(hits.getJSONObject(0).getJSONArray("likes:15m").getString(0)), 10);
    Clock.setPredefinedTimeInMinutes(initialTime + 20);
    timeAggregatedActivityValues1.getAggregatesUpdateJob().run();
    timeAggregatedActivityValues2.getAggregatesUpdateJob().run();
    req = "{ \"sort\":[{\"likes:15m\":\"desc\"}]}";
    res = TestSensei.search(new JSONObject(req));
    hits = res.getJSONArray("hits");
    System.out.println(res.toString(1));
    assertEquals(Integer.parseInt(hits.getJSONObject(0).getJSONArray("likes:5m").getString(0)), 0);
    assertEquals(Integer.parseInt(hits.getJSONObject(0).getJSONArray("likes:15m").getString(0)), 5);
    assertEquals(Integer.parseInt(hits.getJSONObject(1).getJSONArray("likes:15m").getString(0)), 4);
    assertEquals(Integer.parseInt(hits.getJSONObject(2).getJSONArray("likes:15m").getString(0)), 3);
  }
  public void test4OpeningTheNewActivityFieldValues() throws Exception {
    final CompositeActivityValues inMemoryColumnData1 = CompositeActivityManager.cachedInstances.get(1).activityValues;
    inMemoryColumnData1.flush();
    inMemoryColumnData1.syncWithPersistentVersion(String.valueOf(15019));
    String absolutePath = SenseiStarter.IndexDir + "/test/node1/" + "activity/";
    CompositeActivityValues compositeActivityValues = CompositeActivityValues.readFromFile(absolutePath, java.util.Arrays.asList("likes"), Collections.EMPTY_LIST, ZoieConfig.DEFAULT_VERSION_COMPARATOR);
    assertEquals(9, compositeActivityValues.getValueByUID(1L, "likes"));
    assertEquals(9, inMemoryColumnData1.getValueByUID(1L, "likes"));
  }
 

  private synchronized TimeAggregatedActivityValues clear(final CompositeActivityValues inMemoryColumnData1) throws Exception {
    final TimeAggregatedActivityValues timeAggregatedActivityValues = (TimeAggregatedActivityValues)inMemoryColumnData1.getActivityValuesMap().get("likes");
    timeAggregatedActivityValues.getAggregatesUpdateJob().stop();
    timeAggregatedActivityValues.getAggregatesUpdateJob().awaitTermination();
    Thread.sleep(1000);
    for (int i = 0; i <= timeAggregatedActivityValues.maxIndex; i ++) {
      timeAggregatedActivityValues.getDefaultIntValues().fieldValues[i] = 0;
      timeAggregatedActivityValues.getTimeActivities().reset(i);
      for (ActivityIntValues activityIntValues : timeAggregatedActivityValues.getValuesMap().values()) {
        activityIntValues.fieldValues[i] = 0;
      }
    }
    return timeAggregatedActivityValues;
  }
}
