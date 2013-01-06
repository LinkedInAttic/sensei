package com.senseidb.indexing.activity;

import java.util.Collections;

import junit.framework.TestCase;

import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Ignore;

import proj.zoie.api.ZoieSegmentReader;
import proj.zoie.impl.indexing.ZoieConfig;

import com.senseidb.conf.SenseiSchema;
import com.senseidb.conf.SenseiSchema.FieldDefinition;
import com.senseidb.gateway.file.FileDataProviderWithMocks;

import com.senseidb.indexing.activity.primitives.ActivityIntValues;
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
    
    inMemoryColumnData1 = CompositeActivityManager.cachedInstances.get(1).activityValues;
    inMemoryColumnData2 = CompositeActivityManager.cachedInstances.get(2).activityValues;
    
    ActivityIntValuesSynchronizedDecorator.decorate((TimeAggregatedActivityValues) inMemoryColumnData1.getActivityValuesMap().get("likes"));
    ActivityIntValuesSynchronizedDecorator.decorate((TimeAggregatedActivityValues) inMemoryColumnData2.getActivityValuesMap().get("likes"));
    initialVersion = FileDataProviderWithMocks.instances.get(0).getOffset();
    initialVersion = Math.max(initialVersion, FileDataProviderWithMocks.instances.get(1).getOffset());    
    initialVersion--;
    expectedVersion = initialVersion;

  }
  
  
  public void test1SendUpdatesAndSort() throws Exception {
    String req = "{ \"sort\":[{\"aggregated-likes\":\"desc\"}]}";
    JSONObject res = TestSensei.search(new JSONObject(req));
    JSONArray hits = res.getJSONArray("hits");
    assertEquals(Integer.parseInt(hits.getJSONObject(0).getJSONArray("aggregated-likes").getString(0)), 1);
    for (int i = 0; i < 10; i++) {
      FileDataProviderWithMocks.add(new JSONObject().put("id", 10L + i).put(SenseiSchema.EVENT_TYPE_FIELD, SenseiSchema.EVENT_TYPE_UPDATE).put("likes", "+" + (10 + i)));
      expectedVersion++;
    }
    syncWithVersion(expectedVersion); 
    req = "{\"selections\": [{\"range\": {\"aggregated-likes\": {\"from\": 18, \"include_lower\": true}}}], \"sort\":[{\"aggregated-likes\":\"desc\"}]}";
    System.out.println("!!!search");
    res = TestSensei.search(new JSONObject(req));
    hits = res.getJSONArray("hits");   
      assertEquals(CompositeActivityManager.cachedInstances.get(1).activityValues.getIntValueByUID(19, "likes"), 20);
      assertEquals(Integer.parseInt(hits.getJSONObject(0).getJSONArray("aggregated-likes").getString(0)), 20);
      assertEquals(CompositeActivityManager.cachedInstances.get(1).activityValues.getIntValueByUID(18, "likes"), 19);
      assertEquals(Integer.parseInt(hits.getJSONObject(1).getJSONArray("aggregated-likes").getString(0)), 19);
      assertEquals(CompositeActivityManager.cachedInstances.get(2).activityValues.getIntValueByUID(17, "likes"), 18);
      assertEquals(Integer.parseInt(hits.getJSONObject(2).getJSONArray("aggregated-likes").getString(0)), 18);
   
  
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
  public void test1CSendUpdatesAndSortFloat() throws Exception {
    String req = "{ \"sort\":[{\"reputation\":\"desc\"}]}";
    JSONObject res = TestSensei.search(new JSONObject(req));
    JSONArray hits = res.getJSONArray("hits");
    assertEquals(Integer.parseInt(hits.getJSONObject(0).getJSONArray("reputation").getString(0)), 0);
    for (int i = 0; i < 10; i++) {
      FileDataProviderWithMocks.add(new JSONObject().put("id", 10L + i).put(SenseiSchema.EVENT_TYPE_FIELD, SenseiSchema.EVENT_TYPE_UPDATE).put("reputation", "+" + (10f + i)));
      expectedVersion++;
    }
    syncWithVersion(expectedVersion); 
    req = "{\"selections\": [{\"range\": {\"reputation\": {\"from\": 18, \"include_lower\": true}}}], \"sort\":[{\"reputation\":\"desc\"}]}";
    System.out.println("!!!search");
    res = TestSensei.search(new JSONObject(req));
    hits = res.getJSONArray("hits");   
      assertEquals(CompositeActivityManager.cachedInstances.get(1).activityValues.getFloatValueByUID(19, "reputation"), 19f);
      assertEquals(Float.parseFloat(hits.getJSONObject(0).getJSONArray("reputation").getString(0)), 19f);
      assertEquals(CompositeActivityManager.cachedInstances.get(1).activityValues.getFloatValueByUID(18, "reputation"), 18f);
      assertEquals(Float.parseFloat(hits.getJSONObject(1).getJSONArray("reputation").getString(0)), 18f);
      
   
  
  }
  public void ntest1DSendUpdatesAndSortLong() throws Exception {
      String req = "{ \"sort\":[{\"modifiedDate\":\"desc\"}]}";
      JSONObject res = TestSensei.search(new JSONObject(req));
      JSONArray hits = res.getJSONArray("hits");
      assertEquals(Long.parseLong(hits.getJSONObject(0).getJSONArray("modifiedDate").getString(0)), 5000000001L);
      assertEquals(Long.parseLong(hits.getJSONObject(1).getJSONArray("modifiedDate").getString(0)), 5000000000L);
      for (int i = 0; i < 10; i++) {
        FileDataProviderWithMocks.add(new JSONObject().put("id", i).put(SenseiSchema.EVENT_TYPE_FIELD, SenseiSchema.EVENT_TYPE_UPDATE).put("modifiedDate", "+1"));
        expectedVersion++;
      }
      syncWithVersion(expectedVersion); 
      req = "{\"selections\": [{\"range\": {\"modifiedDate\": {\"from\": 1, \"include_lower\": true}}}], \"sort\":[{\"modifiedDate\":\"desc\"}]}";
      System.out.println("!!!search");
      res = TestSensei.search(new JSONObject(req));
      hits = res.getJSONArray("hits");   
        assertEquals(Long.parseLong(hits.getJSONObject(0).getJSONArray("modifiedDate").getString(0)), 5000000002L);
        assertEquals(Long.parseLong(hits.getJSONObject(1).getJSONArray("modifiedDate").getString(0)), 5000000001L);
        assertEquals(res.getInt("numhits"), 10);
     
    
    }
  private static void syncWithVersion(final long expectedVersion) {
    final CompositeActivityValues inMemoryColumnData1 = CompositeActivityManager.cachedInstances.get(1).activityValues;
    final CompositeActivityValues inMemoryColumnData2 = CompositeActivityManager.cachedInstances.get(2).activityValues;
    Wait.until(10000, "The activity value wasn't updated", new Wait.Condition() {
      public boolean evaluate() {
        long v1 = Long.parseLong(inMemoryColumnData1.getVersion());
        long v2 = Long.parseLong(inMemoryColumnData2.getVersion());
        return (v1 == expectedVersion || v2 == expectedVersion) && (v1 >= expectedVersion - 1 && v2 >= expectedVersion - 1);
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
        return inMemoryColumnData1.getIntValueByUID(1L, "likes") == 26 || inMemoryColumnData2.getIntValueByUID(1L, "likes") == 26;
      }
    });
    for (int i = 0; i < 5; i++) {
      FileDataProviderWithMocks.add(new JSONObject().put("id", 1L).put(SenseiSchema.EVENT_TYPE_FIELD, SenseiSchema.EVENT_TYPE_UPDATE).put("likes", "+5"));
      expectedVersion++;
    }
    Wait.until(10000, "The activity value wasn't updated", new Wait.Condition() {
      public boolean evaluate() {
        return inMemoryColumnData1.getIntValueByUID(1L, "likes") == 51 || inMemoryColumnData2.getIntValueByUID(1L, "likes") == 51;
      }
    });
  }
  public void test2bSendUpdatesAndSortLong() throws Exception {
      String req = "{ \"sort\":[{\"modifiedDate\":\"desc\"}]}";
      JSONObject res = TestSensei.search(new JSONObject(req));
      JSONArray hits = res.getJSONArray("hits");
      assertEquals(Long.parseLong(hits.getJSONObject(0).getJSONArray("modifiedDate").getString(0)), 5000000001L);
      assertEquals(Long.parseLong(hits.getJSONObject(1).getJSONArray("modifiedDate").getString(0)), 5000000000L);
      for (int i = 0; i < 10; i++) {
        FileDataProviderWithMocks.add(new JSONObject().put("id", i).put(SenseiSchema.EVENT_TYPE_FIELD, SenseiSchema.EVENT_TYPE_UPDATE).put("modifiedDate", "+1"));
        expectedVersion++;
      }
      syncWithVersion(expectedVersion); 
      req = "{\"selections\": [{\"range\": {\"modifiedDate\": {\"from\": 1, \"include_lower\": true}}}], \"sort\":[{\"modifiedDate\":\"desc\"}]}";
      System.out.println("!!!search");
      res = TestSensei.search(new JSONObject(req));
      hits = res.getJSONArray("hits");   
        assertEquals(Long.parseLong(hits.getJSONObject(0).getJSONArray("modifiedDate").getString(0)), 5000000002L);
        assertEquals(Long.parseLong(hits.getJSONObject(1).getJSONArray("modifiedDate").getString(0)), 5000000001L);
        assertEquals(res.getInt("numhits"), 10);
        req = "{\"selections\": [{\"range\": {\"modifiedDate\": {\"from\": 5000000002, \"include_lower\": true}}}], \"sort\":[{\"modifiedDate\":\"desc\"}]}";
        System.out.println("!!!search");
        res = TestSensei.search(new JSONObject(req));
        //System.out.println("!!!"+ res.toString(1));
        assertEquals(res.getInt("numhits"), 1);
    }
  
  public void test3AggregatesIntegrationTest() throws Exception {
    final CompositeActivityValues inMemoryColumnData1 = CompositeActivityManager.cachedInstances.get(1).activityValues;
    final CompositeActivityValues inMemoryColumnData2 = CompositeActivityManager.cachedInstances.get(2).activityValues;
    final TimeAggregatedActivityValues timeAggregatedActivityValues1 = clear(inMemoryColumnData1);
    final TimeAggregatedActivityValues timeAggregatedActivityValues2 = clear(inMemoryColumnData2);
    for (ActivityIntValues activityIntValues : timeAggregatedActivityValues1.getValuesMap().values()) {
      assertEquals(0, activityIntValues.getIntValue(0));
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
      syncWithVersion(expectedVersion);
    }    
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
    //testing deletes
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
    inMemoryColumnData1.flush();
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
  
  public void test5bIncreaseNonExistingActivityValue() throws Exception {
    final CompositeActivityManager inMemoryColumnData1 = CompositeActivityManager.cachedInstances.get(1);
    final CompositeActivityManager inMemoryColumnData2 = CompositeActivityManager.cachedInstances.get(2);    
    
    String req = "{\"query\": {\"ids\": {\"values\": [\"14999\"], \"excludes\": [\"2\"]}}}";
    JSONObject res = TestSensei.search(new JSONObject(req));
    FileDataProviderWithMocks.add(new JSONObject().put("id", 14999).put(SenseiSchema.EVENT_TYPE_FIELD, SenseiSchema.EVENT_TYPE_UPDATE).put("likes", "+" + 100));
    expectedVersion++;
    //inMemoryColumnData1.getActivityValues().syncWithVersion(String.valueOf(expectedVersion));
    inMemoryColumnData2.getActivityValues().syncWithVersion(String.valueOf(expectedVersion));
    req = "{ \"size\":1, \"sort\":[{\"aggregated-likes:2w\":\"desc\"}]}";
   // Thread.sleep(2000);
    res = TestSensei.search(new JSONObject(req));
    JSONArray hits = res.getJSONArray("hits");
    assertEquals(Integer.parseInt(hits.getJSONObject(0).getJSONArray("likes").getString(0)), 100);
    req = "{ \"size\":1, \"sort\":[{\"aggregated-likes\":\"desc\"}]}";
    res = TestSensei.search(new JSONObject(req));
    hits = res.getJSONArray("hits");
    assertEquals(Integer.parseInt(hits.getJSONObject(0).getJSONArray("aggregated-likes").getString(0)), 100);
    
  }

 public void test7RelevanceActivity() throws Exception {
    
    FileDataProviderWithMocks.add(new JSONObject().put("id", 501).put(SenseiSchema.EVENT_TYPE_FIELD, SenseiSchema.EVENT_TYPE_UPDATE).put("likes", 100000));
    expectedVersion++;
    syncWithVersion(expectedVersion); 
    
    {
      String req = "{\"sort\":[\"_score\"],\"query\":{\"query_string\":{\"query\":\"\",\"relevance\":{\"model\":{\"function_params\":[\"_INNER_SCORE\",\"thisYear\",\"year\",\"goodYear\",\"mileageWeight\",\"mileage\"],\"facets\":{\"int\":[\"year\",\"mileage\"],\"long\":[\"groupid\"]},\"function\":\" if(mileageWeight.containsKey(mileage)) return 10000+mileageWeight.get(mileage); if(goodYear.contains(year)) return (float)Math.exp(2d);   if(year==thisYear) return 87f   ; return  _INNER_SCORE;\",\"variables\":{\"map_int_float\":[\"mileageWeight\"],\"set_int\":[\"goodYear\"],\"int\":[\"thisYear\"]}},\"values\":{\"thisYear\":2001,\"mileageWeight\":{\"11400\":777.9, \"11000\":10.2},\"goodYear\":[1996,1997]}}}},\"fetchStored\":false,\"from\":0,\"explain\":false,\"size\":6}";
      JSONObject res = TestSensei.search(new JSONObject(req));
      assertEquals("numhits is wrong", 15000, res.getInt("numhits"));
    }

    {
      String req = "{\"sort\":[\"_score\"],\"query\":{\"query_string\":{\"query\":\"\",\"relevance\":{\"model\":{\"function_params\":[\"_INNER_SCORE\",\"thisYear\",\"year\",\"goodYear\",\"mileageWeight\",\"mileage\",\"likes\"],\"facets\":{\"int\":[\"year\",\"mileage\"],\"long\":[\"groupid\"],\"aint\":[\"likes\"]},\"function\":\" if(mileageWeight.containsKey(mileage)) return 10000+mileageWeight.get(mileage); if(goodYear.contains(year)) return (float)Math.exp(2d);   if(year==thisYear) return 87f   ; if(likes> _INNER_SCORE) return (float)likes;return  _INNER_SCORE;\",\"variables\":{\"map_int_float\":[\"mileageWeight\"],\"set_int\":[\"goodYear\"],\"int\":[\"thisYear\"]}},\"values\":{\"thisYear\":2001,\"mileageWeight\":{\"11400\":777.9, \"11000\":10.2},\"goodYear\":[1996,1997]}}}},\"fetchStored\":false,\"from\":0,\"explain\":false,\"size\":6}";
      JSONObject res = TestSensei.search(new JSONObject(req));
      assertEquals("numhits is wrong", 15000, res.getInt("numhits"));
      
      JSONArray hits = res.getJSONArray("hits");
      JSONObject firstHit = hits.getJSONObject(0);
      JSONObject secondHit = hits.getJSONObject(1);
      
      double firstScore = firstHit.getDouble("_score");
      double secondScore = secondHit.getDouble("_score");
      
      double delta1 = firstScore - 100000;
      double delta2 = secondScore - 10777.900390625;
      
      assertEquals("score for first is not correct. delta is: " + delta1, true, Math.abs(delta1) < 0.001 );
      assertEquals("score for second is not correct." , true, Math.abs(delta2) < 1 );
      
      int first_aggregated_likes_2w = firstHit.getJSONArray("aggregated-likes:2w").getInt(0);
      int first_aggregated_likes_12h = firstHit.getJSONArray("aggregated-likes:12h").getInt(0);
      
      //int second_aggregated_likes_2w = secondHit.getJSONArray("aggregated-likes:2w").getInt(0);
      //int second_aggregated_likes_12h = secondHit.getJSONArray("aggregated-likes:12h").getInt(0);
      
      assertEquals("first hit does not have correct aggregated value.", true, first_aggregated_likes_2w==100000);
      assertEquals("first hit does not have correct aggregated value.", true, first_aggregated_likes_12h==100000);
    }

  }  
 
public void test5PurgeUnusedActivities() throws Exception {
  final CompositeActivityManager inMemoryColumnData1 = CompositeActivityManager.cachedInstances.get(1);
  final CompositeActivityManager inMemoryColumnData2 = CompositeActivityManager.cachedInstances.get(2);
  int count1 =  inMemoryColumnData1.getPurgeUnusedActivitiesJob().purgeUnusedActivityIndexes();
  int count2 = inMemoryColumnData2.getPurgeUnusedActivitiesJob().purgeUnusedActivityIndexes();
  assertEquals(0, count1 + count2);
  for (int j = 0; j < 10; j ++) {
    inMemoryColumnData1.acceptEvent(new JSONObject().put("id", j + 30000).put(SenseiSchema.EVENT_TYPE_FIELD, SenseiSchema.EVENT_TYPE_UPDATE).put("likes", "+" + 1), String.valueOf(expectedVersion + 1));
    inMemoryColumnData2.acceptEvent(new JSONObject().put("id", j + 30000).put(SenseiSchema.EVENT_TYPE_FIELD, SenseiSchema.EVENT_TYPE_UPDATE).put("likes", "+" + 1), String.valueOf(expectedVersion + 1));
    expectedVersion++;
  }
  FileDataProviderWithMocks.resetOffset(expectedVersion);
  inMemoryColumnData1.getActivityValues().syncWithVersion(String.valueOf(expectedVersion));
 
  count1 =  inMemoryColumnData1.getPurgeUnusedActivitiesJob().purgeUnusedActivityIndexes();
  count2 = inMemoryColumnData2.getPurgeUnusedActivitiesJob().purgeUnusedActivityIndexes();
  assertEquals(0, count1 + count2);
  inMemoryColumnData1.activityValues.recentlyAddedUids.clear();
  inMemoryColumnData2.activityValues.recentlyAddedUids.clear();
  count1 =  inMemoryColumnData1.getPurgeUnusedActivitiesJob().purgeUnusedActivityIndexes();
  count2 = inMemoryColumnData2.getPurgeUnusedActivitiesJob().purgeUnusedActivityIndexes();
  assertEquals(20, count1 + count2);
}
  
  public void test6OpeningTheNewActivityFieldValues() throws Exception {
    final CompositeActivityValues inMemoryColumnData1 = CompositeActivityManager.cachedInstances.get(1).activityValues;
    inMemoryColumnData1.flush();
    inMemoryColumnData1.syncWithPersistentVersion(String.valueOf(expectedVersion - 1));
    inMemoryColumnData2.flush();
    inMemoryColumnData2.syncWithPersistentVersion(String.valueOf(expectedVersion - 1));
    String absolutePath = SenseiStarter.IndexDir + "/node1/" + "activity/";
    FieldDefinition fieldDefinition = getLikesFieldDefinition();

    CompositeActivityValues compositeActivityValues =  CompositeActivityValues.createCompositeValues(ActivityPersistenceFactory.getInstance(absolutePath, new ActivityConfig()), java.util.Arrays.asList(fieldDefinition), Collections.EMPTY_LIST, ZoieConfig.DEFAULT_VERSION_COMPARATOR);

    assertEquals(1, compositeActivityValues.getIntValueByUID(1L, "likes"));
    assertEquals(1, inMemoryColumnData1.getIntValueByUID(1L, "likes"));
  }
  private static FieldDefinition getLikesFieldDefinition() {
    
    return getIntFieldDefinition("likes");
  }
  public static FieldDefinition getIntFieldDefinition(String name) {
    FieldDefinition fieldDefinition = new FieldDefinition();
    fieldDefinition.name = name;
    fieldDefinition.type = int.class;
    fieldDefinition.isActivity = true;
    return fieldDefinition;
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
