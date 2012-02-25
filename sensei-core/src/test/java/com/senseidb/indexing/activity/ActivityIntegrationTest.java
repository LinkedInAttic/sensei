package com.senseidb.indexing.activity;

import junit.framework.TestCase;

import org.apache.log4j.Logger;
import org.json.JSONObject;

import proj.zoie.impl.indexing.ZoieConfig;

import com.senseidb.gateway.file.FileDataProviderWithMocks;
import com.senseidb.search.req.mapred.TestMapReduce;
import com.senseidb.svc.api.SenseiService;
import com.senseidb.test.SenseiStarter;

public class ActivityIntegrationTest extends TestCase {

  private static final Logger logger = Logger.getLogger(ActivityIntegrationTest.class);
 
  static {
    SenseiStarter.start("test-conf/node1", "test-conf/node2");  
  }

  public void test1SendUpdateAndCheckIfItsPersisted() throws Exception {
    for (int i = 0; i < 5; i++) {
      FileDataProviderWithMocks.add(new JSONObject().put("id", 1L).put("_type", "update").put("likes", "+5").put("color", "blue"));
    }
    final ActivityFieldValues inMemoryColumnData1 = CompositeActivityManager.cachedInstances.get(1).columnsMap.get("likes");
    final ActivityFieldValues inMemoryColumnData2 = CompositeActivityManager.cachedInstances.get(2).columnsMap.get("likes");
    Wait.waitUntil(10000, "The activity value wasn't updated", new Wait.Condition() {
      public boolean evaluate() {
        return inMemoryColumnData1.getValueByUID(1L) == 26 || inMemoryColumnData2.getValueByUID(1L) == 26;
      }
    });
    for (int i = 0; i < 5; i++) {
      FileDataProviderWithMocks.add(new JSONObject().put("id", 1L).put("_type", "update").put("likes", "+5"));
    }
    Wait.waitUntil(10000, "The activity value wasn't updated", new Wait.Condition() {
      public boolean evaluate() {
        return inMemoryColumnData1.getValueByUID(1L) == 51 || inMemoryColumnData2.getValueByUID(1L) == 51;
      }
    });
  }

  public void test2OpeningTheNewActivityFieldValues() throws Exception {
    final ActivityFieldValues inMemoryColumnData1 = CompositeActivityManager.cachedInstances.get(1).columnsMap.get("likes");
    inMemoryColumnData1.flush();
    inMemoryColumnData1.syncWithPersistentVersion(String.valueOf(15009));
    FileDataProviderWithMocks.add(new JSONObject().put("id", 0).put("_type", "update").put("likes", "+5"));
    String absolutePath = SenseiStarter.IndexDir + "/test/node1/" + "activity/";
    ActivityFieldValues activityFieldValues = ActivityFieldValues.readFromFile(absolutePath, "likes", ZoieConfig.DEFAULT_VERSION_COMPARATOR);
    assertEquals(51, activityFieldValues.getValueByUID(1L));
  }
}
