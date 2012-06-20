package com.senseidb.indexing.activity;

import java.util.Collections;

import junit.framework.TestCase;

import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import proj.zoie.impl.indexing.ZoieConfig;

import com.senseidb.conf.SenseiSchema;
import com.senseidb.gateway.file.FileDataProviderWithMocks;
import com.senseidb.indexing.activity.facet.ActivityRangeFacetHandler;
import com.senseidb.indexing.activity.time.ActivityIntValuesSynchronizedDecorator;
import com.senseidb.indexing.activity.time.Clock;
import com.senseidb.indexing.activity.time.TimeAggregatedActivityValues;
import com.senseidb.test.SenseiStarter;
import com.senseidb.test.TestSensei;

public class ActivityIntegrationTest extends TestCase {
  private static final Logger logger = Logger.getLogger(ActivityIntegrationTest.class);
  private static long initialVersion;
  private static long expectedVersion;
  private static CompositeActivityValues inMemoryColumnData1;
  private static CompositeActivityValues inMemoryColumnData2;
  static {
    
    SenseiStarter.start("test-conf/node1", "test-conf/node2");
    initialVersion = Long.parseLong(CompositeActivityManager.cachedInstances.get(1).activityValues.getVersion());
    initialVersion = Math.max(initialVersion, Long.parseLong(CompositeActivityManager.cachedInstances.get(2).activityValues.getVersion()));    
    expectedVersion = initialVersion;
    inMemoryColumnData1 = CompositeActivityManager.cachedInstances.get(1).activityValues;
    inMemoryColumnData2 = CompositeActivityManager.cachedInstances.get(2).activityValues;
    ActivityIntValuesSynchronizedDecorator.decorate((TimeAggregatedActivityValues) inMemoryColumnData1.getActivityValuesMap().get("likes"));
    ActivityIntValuesSynchronizedDecorator.decorate((TimeAggregatedActivityValues) inMemoryColumnData2.getActivityValuesMap().get("likes"));
  }
  
  
  public void test1SendUpdatesAndSort() throws Exception {
    for (int i = 0; i < 10; i++) {
      FileDataProviderWithMocks.add(new JSONObject().put("id", 10L + i).put(SenseiSchema.EVENT_TYPE_FIELD, SenseiSchema.EVENT_TYPE_UPDATE).put("likes", "+" + (10 + i)));
      expectedVersion++;
    }
    syncWithVersion(expectedVersion); 
    String req = "{\"selections\": [{\"range\": {\"aggregated-likes\": {\"from\": 18, \"include_lower\": true}}}], \"sort\":[{\"aggregated-likes\":\"desc\"}]}";
    JSONObject res = TestSensei.search(new JSONObject(req));
    JSONArray hits = res.getJSONArray("hits");
    assertEquals(Integer.parseInt(hits.getJSONObject(0).getJSONArray("aggregated-likes").getString(0)), 20);
    assertEquals(Integer.parseInt(hits.getJSONObject(1).getJSONArray("aggregated-likes").getString(0)), 19);
    assertEquals(Integer.parseInt(hits.getJSONObject(2).getJSONArray("aggregated-likes").getString(0)), 18);
   System.out.println("!!!" + res.toString(1));
  }
  public void test1bSendUpdatesAndSort() throws Exception {   
    String req = "{\"selections\": [{\"range\": {\"likes\": {\"from\": 18, \"include_lower\": true}}}], \"sort\":[{\"likes\":\"desc\"}]}";
    JSONObject res = TestSensei.search(new JSONObject(req));
    JSONArray hits = res.getJSONArray("hits");
    assertEquals(Integer.parseInt(hits.getJSONObject(0).getJSONArray("likes").getString(0)), 20);
    assertEquals(Integer.parseInt(hits.getJSONObject(1).getJSONArray("likes").getString(0)), 19);
    assertEquals(Integer.parseInt(hits.getJSONObject(2).getJSONArray("likes").getString(0)), 18);
   System.out.println("!!!" + res.toString(1));
  }
  private void syncWithVersion(final long expectedVersion) {
    final CompositeActivityValues inMemoryColumnData1 = CompositeActivityManager.cachedInstances.get(1).activityValues;
    final CompositeActivityValues inMemoryColumnData2 = CompositeActivityManager.cachedInstances.get(2).activityValues;
    Wait.until(10000, "The activity value wasn't updated", new Wait.Condition() {
      public boolean evaluate() {
        return inMemoryColumnData1.getVersion().equals(String.valueOf(expectedVersion)) || inMemoryColumnData2.getVersion().equals(String.valueOf(expectedVersion));
      }
    });
  }
  
  public void test2SendUpdateAndCheckIfItsPersisted() throws Exception {
    for (int i = 0; i < 5; i++) {
      FileDataProviderWithMocks.add(new JSONObject().put("id", 1L).put(SenseiSchema.EVENT_TYPE_FIELD, SenseiSchema.EVENT_TYPE_UPDATE).put("likes", "+5"));
      expectedVersion++;
    }
    final CompositeActivityValues inMemoryColumnData1 = CompositeActivityManager.cachedInstances.get(1).activityValues;
    final CompositeActivityValues inMemoryColumnData2 = CompositeActivityManager.cachedInstances.get(2).activityValues;
    Wait.until(10000, "The activity value wasn't updated", new Wait.Condition() {
      public boolean evaluate() {
        return inMemoryColumnData1.getValueByUID(1L, "likes") == 26 || inMemoryColumnData2.getValueByUID(1L, "likes") == 26;
      }
    });
    for (int i = 0; i < 5; i++) {
      FileDataProviderWithMocks.add(new JSONObject().put("id", 1L).put(SenseiSchema.EVENT_TYPE_FIELD, SenseiSchema.EVENT_TYPE_UPDATE).put("likes", "+5"));
      expectedVersion++;
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
      for (int i = 0; i < activityIntValues.getFieldValues().length; i ++) {
        assertEquals("" + i, 0, activityIntValues.getFieldValues()[i]);
      }
    }  
    int initialTime = Clock.getCurrentTimeInMinutes();
    for (int i = 0; i < 10; i++) {
      final int uid = i;
      Clock.setPredefinedTimeInMinutes(Clock.getCurrentTimeInMinutes() + 1);
      for (int j = 0; j < 10 - i; j ++) {
        FileDataProviderWithMocks.add(new JSONObject().put("id", j).put(SenseiSchema.EVENT_TYPE_FIELD, SenseiSchema.EVENT_TYPE_UPDATE).put("likes", "+1"));
        expectedVersion++;
      }
      if (uid < 9)Wait.until(10000, "" + i, new Wait.Condition() {
        @Override
        public boolean evaluate() { 
          synchronized(inMemoryColumnData1.getActivityIntValues("likes").getFieldValues()) {
            return inMemoryColumnData1.getValueByUID(1, "likes") == uid + 1;
          }
        }
      });   
    }
    Wait.until(10000, "", new Wait.Condition() {
      @Override
      public boolean evaluate() { 
        synchronized(((TimeAggregatedActivityValues)inMemoryColumnData1.getActivityValuesMap().get("likes")).getValuesMap().get("15m").getFieldValues()) {
          return inMemoryColumnData1.getValueByUID(0, "likes:15m") == 10;
        }
      }
    });   
    String req = "{\"selections\": [{\"range\": {\"aggregated-likes:2w\": {\"from\": 8, \"include_lower\": true}}}], \"sort\":[{\"aggregated-likes:2w\":\"desc\"}]}";
    JSONObject res = TestSensei.search(new JSONObject(req));     
   
    JSONArray hits = res.getJSONArray("hits");
    assertEquals(Integer.parseInt(hits.getJSONObject(0).getJSONArray("aggregated-likes:2w").getString(0)), 10);
    assertEquals(Integer.parseInt(hits.getJSONObject(1).getJSONArray("aggregated-likes:2w").getString(0)), 9);
    assertEquals(Integer.parseInt(hits.getJSONObject(2).getJSONArray("aggregated-likes:2w").getString(0)), 8);
    Clock.setPredefinedTimeInMinutes(initialTime + 11);
    timeAggregatedActivityValues1.getAggregatesUpdateJob().run();
    timeAggregatedActivityValues2.getAggregatesUpdateJob().run();
    req = "{ \"sort\":[{\"aggregated-likes:5m\":\"desc\"}]}";
     res = TestSensei.search(new JSONObject(req));
     hits = res.getJSONArray("hits");
    assertEquals(Integer.parseInt(hits.getJSONObject(0).getJSONArray("aggregated-likes:5m").getString(0)), 4);
    assertEquals(Integer.parseInt(hits.getJSONObject(1).getJSONArray("aggregated-likes:5m").getString(0)), 3);
    assertEquals(Integer.parseInt(hits.getJSONObject(2).getJSONArray("aggregated-likes:5m").getString(0)), 2);
    assertEquals(Integer.parseInt(hits.getJSONObject(0).getJSONArray("aggregated-likes:15m").getString(0)), 10);
    
    Clock.setPredefinedTimeInMinutes(initialTime + 20);
    timeAggregatedActivityValues1.getAggregatesUpdateJob().run();
    timeAggregatedActivityValues2.getAggregatesUpdateJob().run();
    req = "{ \"sort\":[{\"aggregated-likes:15m\":\"desc\"}]}";

    res = TestSensei.search(new JSONObject(req));
    hits = res.getJSONArray("hits");
   
    assertEquals(Integer.parseInt(hits.getJSONObject(0).getJSONArray("aggregated-likes:5m").getString(0)), 0);
    assertEquals(Integer.parseInt(hits.getJSONObject(0).getJSONArray("aggregated-likes:15m").getString(0)), 5);
    assertEquals(Integer.parseInt(hits.getJSONObject(1).getJSONArray("aggregated-likes:15m").getString(0)), 4);
    assertEquals(Integer.parseInt(hits.getJSONObject(2).getJSONArray("aggregated-likes:15m").getString(0)), 3);
    assertEquals(Integer.parseInt(hits.getJSONObject(3).getJSONArray("aggregated-likes:15m").getString(0)), 2);
    assertEquals(Integer.parseInt(hits.getJSONObject(4).getJSONArray("aggregated-likes:15m").getString(0)), 1);
    assertEquals(Integer.parseInt(hits.getJSONObject(5).getJSONArray("aggregated-likes:15m").getString(0)), 0);
    inMemoryColumnData1.delete(0L);
    inMemoryColumnData2.delete(0L);
    req = "{ \"sort\":[{\"aggregated-likes:15m\":\"desc\"}]}";
    //testign deletes
    res = TestSensei.search(new JSONObject(req));
    hits = res.getJSONArray("hits");
    
    assertEquals(Integer.parseInt(hits.getJSONObject(0).getJSONArray("aggregated-likes:15m").getString(0)), 4);
    assertEquals(Integer.parseInt(hits.getJSONObject(1).getJSONArray("aggregated-likes:15m").getString(0)), 3);
    assertEquals(Integer.parseInt(hits.getJSONObject(2).getJSONArray("aggregated-likes:15m").getString(0)), 2);
   
  }
  public void test6AddDeleteAddAgainAndQuery() throws Exception {
    final CompositeActivityValues inMemoryColumnData1 = CompositeActivityManager.cachedInstances.get(1).activityValues;
    final CompositeActivityValues inMemoryColumnData2 = CompositeActivityManager.cachedInstances.get(2).activityValues;
    final TimeAggregatedActivityValues timeAggregatedActivityValues1 = clear(inMemoryColumnData1);
    final TimeAggregatedActivityValues timeAggregatedActivityValues2 = clear(inMemoryColumnData2);
    for (int i = 0; i < 10; i ++) {
      FileDataProviderWithMocks.add(new JSONObject().put("id", i).put(SenseiSchema.EVENT_TYPE_FIELD, SenseiSchema.EVENT_TYPE_UPDATE).put("likes", "+" + i));
      expectedVersion++;
    }
    inMemoryColumnData1.syncWithVersion(String.valueOf(expectedVersion));
    String req = "{ \"sort\":[{\"aggregated-likes:15m\":\"desc\"}]}";
    JSONObject res = TestSensei.search(new JSONObject(req));
    JSONArray hits = res.getJSONArray("hits");
    assertEquals(Integer.parseInt(hits.getJSONObject(0).getJSONArray("aggregated-likes:15m").getString(0)), 9);
    assertEquals(Integer.parseInt(hits.getJSONObject(0).getString("_uid")), 9);
    assertEquals(Integer.parseInt(hits.getJSONObject(1).getJSONArray("aggregated-likes:15m").getString(0)), 8);
    assertEquals(Integer.parseInt(hits.getJSONObject(1).getString("_uid")), 8);
    assertEquals(Integer.parseInt(hits.getJSONObject(2).getJSONArray("aggregated-likes:15m").getString(0)), 7);
    assertEquals(Integer.parseInt(hits.getJSONObject(2).getString("_uid")), 7);
    for (int i = 0; i < 10; i ++) {
      inMemoryColumnData1.delete(i);
      inMemoryColumnData2.delete(i);
    }
    req = "{ \"sort\":[{\"aggregated-likes:15m\":\"desc\"}]}";
    res = TestSensei.search(new JSONObject(req));
    hits = res.getJSONArray("hits");
    assertEquals(Integer.parseInt(hits.getJSONObject(0).getJSONArray("aggregated-likes:15m").getString(0)), 0);
    inMemoryColumnData1.flushDeletes();
    inMemoryColumnData2.flushDeletes();
    Thread.sleep(1000);
    for (int i = 0; i < 10; i ++) {
      FileDataProviderWithMocks.add(new JSONObject().put("id", i).put(SenseiSchema.EVENT_TYPE_FIELD, SenseiSchema.EVENT_TYPE_UPDATE).put("likes", "+" + i));
      expectedVersion++;
    }
    inMemoryColumnData1.syncWithVersion(String.valueOf(expectedVersion));
    req = "{\"selections\": [{\"range\": {\"aggregated-likes:2w\": {\"from\": 5, \"include_lower\": true}}}], \"sort\":[{\"aggregated-likes:2w\":\"desc\"}]}";;
    res = TestSensei.search(new JSONObject(req));
    System.out.println(res.toString(1));
    hits = res.getJSONArray("hits");
    assertTrue(hits.length() > 0);   
  }
  
  public void test5OpeningTheNewActivityFieldValues() throws Exception {
    final CompositeActivityValues inMemoryColumnData1 = CompositeActivityManager.cachedInstances.get(1).activityValues;
    inMemoryColumnData1.flush();
    inMemoryColumnData1.syncWithPersistentVersion(String.valueOf(expectedVersion - 1));
    inMemoryColumnData2.flush();
    inMemoryColumnData2.syncWithPersistentVersion(String.valueOf(expectedVersion - 1));
    String absolutePath = SenseiStarter.IndexDir + "/node1/" + "activity/";
    CompositeActivityValues compositeActivityValues = CompositeActivityValues.readFromFile(absolutePath, java.util.Arrays.asList("likes"), Collections.EMPTY_LIST, ZoieConfig.DEFAULT_VERSION_COMPARATOR);
    assertEquals(1, compositeActivityValues.getValueByUID(1L, "likes"));
    assertEquals(1, inMemoryColumnData1.getValueByUID(1L, "likes"));
  }
 

  private synchronized TimeAggregatedActivityValues clear(final CompositeActivityValues inMemoryColumnData1) throws Exception {
    final TimeAggregatedActivityValues timeAggregatedActivityValues = (TimeAggregatedActivityValues)inMemoryColumnData1.getActivityValuesMap().get("likes");
    timeAggregatedActivityValues.getAggregatesUpdateJob().stop();
    timeAggregatedActivityValues.getAggregatesUpdateJob().awaitTermination();
    Thread.sleep(1000);
    for (int i = 0; i <= timeAggregatedActivityValues.maxIndex; i ++) {
      timeAggregatedActivityValues.getDefaultIntValues().getFieldValues()[i] = 0;
      timeAggregatedActivityValues.getTimeActivities().reset(i);
      for (ActivityIntValues activityIntValues : timeAggregatedActivityValues.getValuesMap().values()) {
        activityIntValues.getFieldValues()[i] = 0;
      }
    }
    return timeAggregatedActivityValues;
  }
}
