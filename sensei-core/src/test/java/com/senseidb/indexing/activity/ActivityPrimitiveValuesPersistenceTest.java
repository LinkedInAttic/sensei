package com.senseidb.indexing.activity;

import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import junit.framework.TestCase;

import org.json.JSONObject;

import proj.zoie.impl.indexing.ZoieConfig;

import com.senseidb.conf.SenseiSchema.FieldDefinition;
import com.senseidb.indexing.activity.primitives.ActivityFloatValues;
import com.senseidb.indexing.activity.primitives.ActivityIntValues;
import com.senseidb.indexing.activity.primitives.ActivityLongValues;
import com.senseidb.test.SenseiStarter;

public class ActivityPrimitiveValuesPersistenceTest extends TestCase {
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
    return "sensei-test/activity";
  }
  @Override
  protected void tearDown() throws Exception {
    File file = new File("sensei-test");
    if (file.exists()) {
      file.deleteOnExit();
      SenseiStarter.rmrf(file);
    }
  }
  public void test1WriteValuesAndReadJustAfterThatInMemmory() throws Exception {
    compositeActivityValues = CompositeActivityValues.createCompositeValues(ActivityPersistenceFactory.getInMemoryInstance(), java.util.Arrays.asList(PurgeUnusedActivitiesJobTest.getLikesFieldDefinition()), Collections.EMPTY_LIST, ZoieConfig.DEFAULT_VERSION_COMPARATOR);
    compositeActivityValues.init();
   int valueCount = 10000;
   for (int i = 0; i < valueCount; i++) { 
     compositeActivityValues.update(10000000000L + i, String.format("%08d", i), toMap(new JSONObject().put("likes", "+1")));
   }    
   compositeActivityValues.flush();
   compositeActivityValues.syncWithPersistentVersion(String.format("%08d", valueCount - 1));
    
   assertEquals("Found " + compositeActivityValues.uidToArrayIndex.size(), valueCount, compositeActivityValues.uidToArrayIndex.size());
   for (int i = 0; i < valueCount; i++) {      
     compositeActivityValues.update(10000000000L + i, String.format("%08d", valueCount + i), toMap(new JSONObject().put("likes","+" + i)));
   }
   compositeActivityValues.flush();
   compositeActivityValues.syncWithPersistentVersion(String.format("%08d", valueCount * 2 - 1));
   compositeActivityValues.close();
   assertEquals(getFieldValues(compositeActivityValues)[0], 1);
   assertEquals(getFieldValues(compositeActivityValues)[3], 4); 
 }
  
  
  public void test1WriteValuesAndReadJustAfterThat() throws Exception {
    
    compositeActivityValues = CompositeActivityValues.createCompositeValues(ActivityPersistenceFactory.getInstance(getDirPath()), java.util.Arrays.asList(PurgeUnusedActivitiesJobTest.getLikesFieldDefinition()), Collections.EMPTY_LIST, ZoieConfig.DEFAULT_VERSION_COMPARATOR);
    
    int valueCount = 100000;
    for (int i = 0; i < valueCount; i++) { 
      compositeActivityValues.update(10000000000L + i, String.format("%08d", i), toMap(new JSONObject().put("likes", "+1")));
    }    
    compositeActivityValues.flush();
    compositeActivityValues.syncWithPersistentVersion(String.format("%08d", valueCount - 1));
    compositeActivityValues.close();
    compositeActivityValues = CompositeActivityValues.createCompositeValues(ActivityPersistenceFactory.getInstance(getDirPath()), java.util.Arrays.asList(PurgeUnusedActivitiesJobTest.getLikesFieldDefinition()), Collections.EMPTY_LIST, ZoieConfig.DEFAULT_VERSION_COMPARATOR);
    
    assertEquals("Found " + compositeActivityValues.uidToArrayIndex.size(), valueCount, compositeActivityValues.uidToArrayIndex.size());
    assertEquals((int)(valueCount * 1.5), getFieldValues(compositeActivityValues).length );
    for (int i = 0; i < valueCount; i++) {      
      compositeActivityValues.update(10000000000L + i, String.format("%08d", valueCount + i), toMap(new JSONObject().put("likes","+" + i)));
    }
    compositeActivityValues.flush();
    compositeActivityValues.syncWithPersistentVersion(String.format("%08d", valueCount * 2 - 1));
    compositeActivityValues.close();
    assertEquals(getFieldValues(compositeActivityValues)[0], 1);
    assertEquals(getFieldValues(compositeActivityValues)[3], 4);
    String prevMetadataString = compositeActivityValues.metadata.toString();
    compositeActivityValues = CompositeActivityValues.createCompositeValues(ActivityPersistenceFactory.getInstance(getDirPath()), java.util.Arrays.asList(PurgeUnusedActivitiesJobTest.getLikesFieldDefinition()), Collections.EMPTY_LIST, ZoieConfig.DEFAULT_VERSION_COMPARATOR);
    assertEquals(getFieldValues(compositeActivityValues)[0], 1);
    assertEquals(getFieldValues(compositeActivityValues)[3], 4);
    assertEquals(prevMetadataString, compositeActivityValues.metadata.toString());
    compositeActivityValues.close();
  }
  public void test1BWriteValuesAndReadJustAfterThatFloatValues() throws Exception {
    String indexDirPath = getDirPath() + 21;
    FieldDefinition fieldDefinition = new FieldDefinition();
    fieldDefinition.name = "reputation";
    fieldDefinition.type = float.class;
    fieldDefinition.isActivity = true;
    compositeActivityValues = CompositeActivityValues.createCompositeValues(ActivityPersistenceFactory.getInstance(indexDirPath), java.util.Arrays.asList(fieldDefinition), Collections.EMPTY_LIST, ZoieConfig.DEFAULT_VERSION_COMPARATOR);
    
    int valueCount = 10000;
    for (int i = 0; i < valueCount; i++) { 
      compositeActivityValues.update(10000000000L + i, String.format("%08d", i), toMap(new JSONObject().put("reputation", "+1.0")));
    }    
    compositeActivityValues.flush();
    compositeActivityValues.syncWithPersistentVersion(String.format("%08d", valueCount - 1));
    compositeActivityValues.close();
    compositeActivityValues = CompositeActivityValues.createCompositeValues(ActivityPersistenceFactory.getInstance(indexDirPath), java.util.Arrays.asList(fieldDefinition), Collections.EMPTY_LIST, ZoieConfig.DEFAULT_VERSION_COMPARATOR);
    
    assertEquals("Found " + compositeActivityValues.uidToArrayIndex.size(), valueCount, compositeActivityValues.uidToArrayIndex.size());
    assertEquals((int)(valueCount * 1.5), getFloatFieldValues(compositeActivityValues).length );
    for (int i = 0; i < valueCount; i++) {      
      compositeActivityValues.update(10000000000L + i, String.format("%08d", valueCount + i), toMap(new JSONObject().put("reputation","+" + (0.0 + i) )));
    }
    compositeActivityValues.flush();
    compositeActivityValues.syncWithPersistentVersion(String.format("%08d", valueCount * 2 - 1));
    compositeActivityValues.close();
    assertEquals(getFloatFieldValues(compositeActivityValues)[0], 1f, 0.5f);
    assertEquals(getFloatFieldValues(compositeActivityValues)[3], 4f, 0.5f);
    compositeActivityValues = CompositeActivityValues.createCompositeValues(ActivityPersistenceFactory.getInstance(indexDirPath), java.util.Arrays.asList(fieldDefinition), Collections.EMPTY_LIST, ZoieConfig.DEFAULT_VERSION_COMPARATOR);
    assertEquals(getFloatFieldValues(compositeActivityValues)[0], 1f, 0.5f);
    assertEquals(getFloatFieldValues(compositeActivityValues)[3], 4f, 0.5f);
    compositeActivityValues.close();
  }
  public void test1CWriteValuesAndReadJustAfterThatLongValues() throws Exception {
      String indexDirPath = getDirPath() + 22;
      FieldDefinition fieldDefinition = new FieldDefinition();
      fieldDefinition.name = "modifiedDate";
      fieldDefinition.type = long.class;
      fieldDefinition.isActivity = true;
      compositeActivityValues = CompositeActivityValues.createCompositeValues(ActivityPersistenceFactory.getInstance(indexDirPath), java.util.Arrays.asList(fieldDefinition), Collections.EMPTY_LIST, ZoieConfig.DEFAULT_VERSION_COMPARATOR);
      
      int valueCount = 10000;
      long testUpdateValue = 5000000000L;
      for (int i = 0; i < valueCount; i++) { 
        compositeActivityValues.update(10000000000L + i, String.format("%08d", i), toMap(new JSONObject().put("modifiedDate", "+" + testUpdateValue)));
      }    
      compositeActivityValues.flush();
      compositeActivityValues.syncWithPersistentVersion(String.format("%08d", valueCount - 1));
      compositeActivityValues.close();
      compositeActivityValues = CompositeActivityValues.createCompositeValues(ActivityPersistenceFactory.getInstance(indexDirPath), java.util.Arrays.asList(fieldDefinition), Collections.EMPTY_LIST, ZoieConfig.DEFAULT_VERSION_COMPARATOR);
      
      assertEquals("Found " + compositeActivityValues.uidToArrayIndex.size(), valueCount, compositeActivityValues.uidToArrayIndex.size());
      assertEquals((int)(valueCount * 1.5), getLongValues(compositeActivityValues).length );
      for (int i = 0; i < valueCount; i++) {      
        compositeActivityValues.update(10000000000L + i, String.format("%08d", valueCount + i), toMap(new JSONObject().put("modifiedDate","+" +  i )));
      }
      compositeActivityValues.flush();
      compositeActivityValues.syncWithPersistentVersion(String.format("%08d", valueCount * 2 - 1));
      compositeActivityValues.close();
      assertEquals(getLongValues(compositeActivityValues)[0], testUpdateValue);
      assertEquals(getLongValues(compositeActivityValues)[3], testUpdateValue + 3);
      compositeActivityValues = CompositeActivityValues.createCompositeValues(ActivityPersistenceFactory.getInstance(indexDirPath), java.util.Arrays.asList(fieldDefinition), Collections.EMPTY_LIST, ZoieConfig.DEFAULT_VERSION_COMPARATOR);
      assertEquals(getLongValues(compositeActivityValues)[0], testUpdateValue);
      assertEquals(getLongValues(compositeActivityValues)[3], testUpdateValue + 3);
      
      compositeActivityValues.close();
    }
public static Map<String, Object> toMap(JSONObject jsonObject) {
  Map<String, Object> ret = new HashMap<String, Object>();
  java.util.Iterator<String> it = jsonObject.keys();
    while (it.hasNext()) {
      String key =  it.next();
      ret.put(key, jsonObject.opt(key));
    }
  return ret;
  }
private int[] getFieldValues(CompositeActivityValues compositeActivityValues){
	return ((ActivityIntValues)compositeActivityValues.valuesMap.get("likes")).fieldValues;
	}
private long[] getLongValues(CompositeActivityValues compositeActivityValues){
    return ((ActivityLongValues)compositeActivityValues.valuesMap.get("modifiedDate")).fieldValues;
    }
private float[] getFloatFieldValues(CompositeActivityValues compositeActivityValues){
  return ((ActivityFloatValues)compositeActivityValues.valuesMap.get("reputation")).fieldValues;
  }
  public void test2WriteDeleteWriteAgain() throws Exception {
    String indexDirPath = getDirPath() + 1;
    dir = new File(indexDirPath);
    dir.mkdirs(); 
    dir.deleteOnExit();
    compositeActivityValues = CompositeActivityValues.createCompositeValues(ActivityPersistenceFactory.getInstance(indexDirPath), java.util.Arrays.asList(PurgeUnusedActivitiesJobTest.getLikesFieldDefinition()), Collections.EMPTY_LIST, ZoieConfig.DEFAULT_VERSION_COMPARATOR);
    final int valueCount = 100000;   
    for (int i = 0; i < valueCount; i++) {
      compositeActivityValues.update(UID_BASE + i, String.format("%08d", valueCount + i), toMap(new JSONObject().put("likes", "+1")));
    }  
    compositeActivityValues.flush();
    compositeActivityValues.syncWithPersistentVersion(String.format("%08d",  valueCount - 1));
    LongList uidsToDelete = new LongArrayList();
    for (int i = 0; i < valueCount; i++) {
      if (i == 2) {
        continue;
      }
      uidsToDelete.add(UID_BASE + i);
      if (i %1000 == 0) {
        compositeActivityValues.delete(uidsToDelete.toLongArray());
        uidsToDelete.clear();
      } 
    }
    compositeActivityValues.flush();
    compositeActivityValues.delete(uidsToDelete.toLongArray());
    compositeActivityValues.flush();
    int notDeletedIndex = compositeActivityValues.uidToArrayIndex.get(UID_BASE + 2);
    final CompositeActivityValues testActivityData = compositeActivityValues;    
    Wait.until(10000L, "", new Wait.Condition() {      
      public boolean evaluate() {
        synchronized (testActivityData.deletedIndexes) {
          return testActivityData.deletedIndexes.size() == valueCount - 1;
        }
      }
    });
    assertEquals(valueCount - 1, compositeActivityValues.deletedIndexes.size());
    assertEquals(1, compositeActivityValues.uidToArrayIndex.size());
    assertFalse(compositeActivityValues.uidToArrayIndex.containsKey(UID_BASE + 1));
    assertEquals(Integer.MIN_VALUE, getFieldValues(compositeActivityValues)[notDeletedIndex - 1]);
    assertEquals(1, getFieldValues(compositeActivityValues)[notDeletedIndex]);
    assertEquals(1, compositeActivityValues.getIntValueByUID(UID_BASE + 2, "likes"));    
    
    compositeActivityValues.flush();
    Thread.sleep(1000L);
    compositeActivityValues.close();
    compositeActivityValues = CompositeActivityValues.createCompositeValues(ActivityPersistenceFactory.getInstance(indexDirPath), java.util.Arrays.asList(PurgeUnusedActivitiesJobTest.getLikesFieldDefinition()), Collections.EMPTY_LIST, ZoieConfig.DEFAULT_VERSION_COMPARATOR);
    
    assertEquals("Found " + compositeActivityValues.metadata.count, valueCount, (int)compositeActivityValues.metadata.count);
    assertEquals(valueCount - 1, compositeActivityValues.deletedIndexes.size());
    assertEquals(1, compositeActivityValues.uidToArrayIndex.size());
   
    assertFalse(compositeActivityValues.uidToArrayIndex.containsKey(UID_BASE + 1));
    assertEquals(1, getFieldValues(compositeActivityValues)[notDeletedIndex]);
    assertEquals(1, compositeActivityValues.getIntValueByUID(UID_BASE + 2, "likes"));
    assertEquals((int)(valueCount * 1.5), getFieldValues(compositeActivityValues).length );
    for (int i = 0; i < valueCount; i++) {      
      compositeActivityValues.update(UID_BASE + i, String.format("%08d", valueCount * 2 + i), toMap(new JSONObject().put("likes", "+" + i)));
    }
    compositeActivityValues.flush();
    compositeActivityValues.syncWithPersistentVersion(String.format("%08d", valueCount * 3 - 1));   
   
    assertEquals(compositeActivityValues.getIntValueByUID(UID_BASE + 0, "likes"), 0);
    assertEquals(compositeActivityValues.getIntValueByUID(UID_BASE + 3, "likes"), 3);
    compositeActivityValues.close();
  }
  public void test3StartWithInconsistentIndexesAddExtraSpaceToCommentFile() throws Exception {
    String indexDirPath = getDirPath() + 5;
    dir = new File(indexDirPath);
    dir.mkdirs(); 
    dir.deleteOnExit();
    System.out.println("Init");
    compositeActivityValues = CompositeActivityValues.createCompositeValues(ActivityPersistenceFactory.getInstance(indexDirPath), java.util.Arrays.asList(PurgeUnusedActivitiesJobTest.getLikesFieldDefinition()), Collections.EMPTY_LIST, ZoieConfig.DEFAULT_VERSION_COMPARATOR);
    final int valueCount = 100;  
    for (int i = 0; i < valueCount; i++) {
      compositeActivityValues.update(UID_BASE + i, String.format("%08d", valueCount + i), toMap(new JSONObject().put("likes", "+1")));
    }  
    compositeActivityValues.flush();
    compositeActivityValues.syncWithPersistentVersion(String.format("%08d",  2*valueCount - 1));   
    compositeActivityValues.close();
    assertTrue(new File(dir, "comments.data").createNewFile());
   
    compositeActivityValues = CompositeActivityValues.createCompositeValues(ActivityPersistenceFactory.getInstance(indexDirPath), java.util.Arrays.asList(PurgeUnusedActivitiesJobTest.getLikesFieldDefinition(), getIntFieldDefinition("comments")), Collections.EMPTY_LIST, ZoieConfig.DEFAULT_VERSION_COMPARATOR);
    assertEquals(0, compositeActivityValues.getIntValueByUID(UID_BASE + valueCount / 2, "comments"));
    compositeActivityValues.close();
  }
  public void test3bStartWithInconsistentIndexesAddExtraSpaceToLongCommentFile() throws Exception {
      String indexDirPath = getDirPath() + 12;
      dir = new File(indexDirPath);
      dir.mkdirs(); 
      dir.deleteOnExit();
      System.out.println("Init");
      compositeActivityValues = CompositeActivityValues.createCompositeValues(ActivityPersistenceFactory.getInstance(indexDirPath), java.util.Arrays.asList(PurgeUnusedActivitiesJobTest.getLikesFieldDefinition()), Collections.EMPTY_LIST, ZoieConfig.DEFAULT_VERSION_COMPARATOR);
      final int valueCount = 700000;  
      for (int i = 0; i < valueCount; i++) {
        compositeActivityValues.update(UID_BASE + i, String.format("%08d", valueCount + i), toMap(new JSONObject().put("likes", "+1")));
      }  
      compositeActivityValues.flush();
      compositeActivityValues.syncWithPersistentVersion(String.format("%08d",  2*valueCount - 1));   
      compositeActivityValues.close();
      assertTrue(new File(dir, "comments.data").createNewFile());
     
      compositeActivityValues = CompositeActivityValues.createCompositeValues(ActivityPersistenceFactory.getInstance(indexDirPath), java.util.Arrays.asList(PurgeUnusedActivitiesJobTest.getLikesFieldDefinition(), getLongFieldDefinition("comments")), Collections.EMPTY_LIST, ZoieConfig.DEFAULT_VERSION_COMPARATOR);
      assertEquals(0, compositeActivityValues.getLongValueByUID(UID_BASE + valueCount / 2, "comments"));
      compositeActivityValues.close();
    }
  public void test4TestForUninsertedValue() throws Exception {
    String indexDirPath = getDirPath() + 3;
    dir = new File(indexDirPath);
    dir.mkdirs(); 
    dir.deleteOnExit();
    compositeActivityValues = CompositeActivityValues.createCompositeValues(ActivityPersistenceFactory.getInstance(indexDirPath), java.util.Arrays.asList(PurgeUnusedActivitiesJobTest.getLikesFieldDefinition(), getIntFieldDefinition("comments")), Collections.EMPTY_LIST, ZoieConfig.DEFAULT_VERSION_COMPARATOR);
    final int valueCount = 100;  
    for (int i = 0; i < valueCount; i++) {
      compositeActivityValues.update(UID_BASE + i, String.format("%08d", valueCount + i), toMap(new JSONObject().put("likes", "+1")));
    }  
    assertEquals(0, compositeActivityValues.getIntValueByUID(UID_BASE + valueCount / 2, "comments"));
    compositeActivityValues.flush();
    compositeActivityValues.syncWithPersistentVersion(String.format("%08d",  2*valueCount - 1));   
    compositeActivityValues.close();    
  }
  public void test5TrimMetadata() throws Exception {
    String indexDirPath = getDirPath() + 4;
    dir = new File(indexDirPath);
    dir.mkdirs(); 
    dir.deleteOnExit();
    compositeActivityValues = CompositeActivityValues.createCompositeValues(ActivityPersistenceFactory.getInstance(indexDirPath), java.util.Arrays.asList(PurgeUnusedActivitiesJobTest.getLikesFieldDefinition()), Collections.EMPTY_LIST, ZoieConfig.DEFAULT_VERSION_COMPARATOR);
    final int valueCount = 100;  
    for (int i = 0; i < valueCount; i++) {
      compositeActivityValues.update(UID_BASE + i, String.format("%08d", valueCount + i), toMap(new JSONObject().put("likes", "+1")));
    }  
    compositeActivityValues.flush();
    compositeActivityValues.syncWithPersistentVersion(String.format("%08d", 2*valueCount - 1));   
    compositeActivityValues.close();
    RandomAccessFile randomAccessFile = new RandomAccessFile(new File(dir, "activity.indexes"), "rw");
    randomAccessFile.setLength(16);
    randomAccessFile.close();
    compositeActivityValues = CompositeActivityValues.createCompositeValues(ActivityPersistenceFactory.getInstance(indexDirPath), java.util.Arrays.asList(PurgeUnusedActivitiesJobTest.getLikesFieldDefinition(), getIntFieldDefinition("comments")), Collections.EMPTY_LIST, ZoieConfig.DEFAULT_VERSION_COMPARATOR);
    assertEquals(1, compositeActivityValues.getIntValueByUID(UID_BASE + 1, "likes"));
    assertEquals(2, compositeActivityValues.uidToArrayIndex.size());
    assertEquals(0, compositeActivityValues.deletedIndexes.size());
    assertEquals(2, compositeActivityValues.metadata.count);
    for (int i = 0; i < valueCount; i++) {
      compositeActivityValues.update(UID_BASE + i, String.format("%08d", 2*valueCount + i), toMap(new JSONObject().put("likes", "+1")));
    }  
    assertEquals(2, compositeActivityValues.getIntValueByUID(UID_BASE , "likes"));
    assertEquals(2, compositeActivityValues.getIntValueByUID(UID_BASE + 1, "likes"));
    assertEquals(1, compositeActivityValues.getIntValueByUID(UID_BASE + valueCount / 2, "likes"));   
    assertEquals(valueCount, compositeActivityValues.uidToArrayIndex.size());
    assertEquals(0, compositeActivityValues.deletedIndexes.size());
    compositeActivityValues.flush();
    compositeActivityValues.syncWithPersistentVersion(String.format("%08d",  3*valueCount - 1));   
    assertEquals(valueCount, compositeActivityValues.metadata.count);
    compositeActivityValues.close();
  }
  public static FieldDefinition getIntFieldDefinition(String name) {
    FieldDefinition fieldDefinition = new FieldDefinition();
    fieldDefinition.name = name;
    fieldDefinition.type = int.class;
    fieldDefinition.isActivity = true;
    return fieldDefinition;
  }
  public static FieldDefinition getLongFieldDefinition(String name) {
      FieldDefinition fieldDefinition = new FieldDefinition();
      fieldDefinition.name = name;
      fieldDefinition.type = long.class;
      fieldDefinition.isActivity = true;
      return fieldDefinition;
    }
}


