package com.senseidb.indexing.activity;

import java.io.File;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import org.json.JSONObject;

import proj.zoie.impl.indexing.ZoieConfig;

import com.senseidb.test.SenseiStarter;

import junit.framework.TestCase;

public class ActivityStressTest extends TestCase {
  private static final long UID_BASE = 10000000000L;
  private File dir;
  private CompositeActivityValues compositeActivityValues; 
  
  
  public void setUp() {
    String pathname = getDirPath();
    SenseiStarter.rmrf(new File("sensei-test"));
    dir = new File(pathname);
    dir.mkdirs();
    
  }
  public static String getDirPath() {
    return "sensei-test/stressTest";
  }
  @Override
  protected void tearDown() throws Exception {
    File file = new File("sensei-test");
    if (file.exists()) {
      file.deleteOnExit();
      SenseiStarter.rmrf(file);
    }
  }
  public void test1() throws Exception {
    int version = 0;
    int iteration = 0;
    int numberOfUniqueDocuments = 0;
    while(true) {
      compositeActivityValues = CompositeActivityValues.createCompositeValues(ActivityPersistenceFactory.getInstance(getDirPath()), java.util.Arrays.asList(PurgeUnusedActivitiesJobTest.getLikesFieldDefinition()), Collections.EMPTY_LIST, ZoieConfig.DEFAULT_VERSION_COMPARATOR);
      if (iteration != 0) {
        long[] arr = new long[5000];
        for (int j = 0; j < arr.length; j++) {
          arr[j] = j;
        }
        compositeActivityValues.delete(arr);
      }
      for (int i = 0; i < 15000; i++) {
        compositeActivityValues.update(i, String.format("%08d", version), ActivityPrimitiveValuesPersistenceTest.toMap(new JSONObject().put("likes", "+1")));
        version++;
      }
      compositeActivityValues.update(1000000 + (numberOfUniqueDocuments++), String.format("%08d", version++), ActivityPrimitiveValuesPersistenceTest.toMap(new JSONObject().put("likes", "+1")));
      long[] arr = new long[5000];
      for (int j = 0; j < arr.length; j++) {
        arr[j] = j + 10000;
      }
      compositeActivityValues.delete(arr);
      if (iteration == 0) {
        close(version);
        iteration++;
        continue;
      }
     // assertEquals(0, compositeActivityValues.deletedIndexes.size());
      compositeActivityValues.flush();
      compositeActivityValues.syncWithPersistentVersion(String.format("%08d", version - 1));
      assertEquals(10000 + numberOfUniqueDocuments, compositeActivityValues.uidToArrayIndex.size());
      assertEquals(10000, compositeActivityValues.deletedIndexes.size());
      assertEquals(20000 + numberOfUniqueDocuments, compositeActivityValues.metadata.count);
      assertEquals(iteration + 1, compositeActivityValues.getIntValueByUID(5000, "likes"));
      assertEquals(iteration + 1, compositeActivityValues.getIntValueByUID(9999, "likes"));
      assertEquals(Integer.MIN_VALUE, compositeActivityValues.getIntValueByUID(10000, "likes"));
      assertEquals(Integer.MIN_VALUE, compositeActivityValues.getIntValueByUID(14999, "likes"));
     assertEquals(1, compositeActivityValues.getIntValueByUID(4999, "likes"));
     assertEquals(1, compositeActivityValues.getIntValueByUID(0, "likes"));
     assertEquals(1, compositeActivityValues.getIntValueByUID(1000000 + numberOfUniqueDocuments - 1, "likes"));
     assertEquals(1, compositeActivityValues.getIntValueByUID(1000000, "likes"));
      assertEquals(String.format("%08d", version - 1), compositeActivityValues.metadata.version);
      compositeActivityValues.close();
      compositeActivityValues.executor.awaitTermination(1, TimeUnit.SECONDS);
      iteration++;
      if (iteration == 10) {
        break;
      }
    }
  }
  public void close(int version) throws InterruptedException {
    compositeActivityValues.flush();
    compositeActivityValues.syncWithPersistentVersion(String.format("%08d", version - 1));
    compositeActivityValues.close();
    compositeActivityValues.executor.awaitTermination(1, TimeUnit.SECONDS);
  }
}
