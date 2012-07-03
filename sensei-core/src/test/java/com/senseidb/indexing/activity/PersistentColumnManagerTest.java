package com.senseidb.indexing.activity;

import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import junit.framework.TestCase;

import org.hsqldb.lib.Iterator;
import org.json.JSONObject;

import proj.zoie.impl.indexing.ZoieConfig;
import scala.actors.threadpool.Arrays;

import com.senseidb.test.SenseiStarter;

public class PersistentColumnManagerTest extends TestCase {
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
    file.deleteOnExit();
    SenseiStarter.rmrf(file);
  }
  public void test1WriteValuesAndReadJustAfterThat() throws Exception {
     compositeActivityValues = CompositeActivityValues.readFromFile(getDirPath(), java.util.Arrays.asList("likes"), Collections.EMPTY_LIST, ZoieConfig.DEFAULT_VERSION_COMPARATOR);
  
    int valueCount = 10000;
    for (int i = 0; i < valueCount; i++) { 
      compositeActivityValues.update(10000000000L + i, String.format("%08d", i), toMap(new JSONObject().put("likes", "+1")));
    }    
    compositeActivityValues.flush();
    compositeActivityValues.syncWithPersistentVersion(String.format("%08d", valueCount - 1));
    compositeActivityValues.close();
    compositeActivityValues = CompositeActivityValues.readFromFile(getDirPath(), java.util.Arrays.asList("likes"), Collections.EMPTY_LIST, ZoieConfig.DEFAULT_VERSION_COMPARATOR);
    
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
    compositeActivityValues = CompositeActivityValues.readFromFile(getDirPath(), java.util.Arrays.asList("likes"), Collections.EMPTY_LIST, ZoieConfig.DEFAULT_VERSION_COMPARATOR);
    assertEquals(getFieldValues(compositeActivityValues)[0], 1);
    assertEquals(getFieldValues(compositeActivityValues)[3], 4);
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
	return ((ActivityIntValues)compositeActivityValues.intValuesMap.get("likes")).fieldValues;
	}
  public void test2WriteDeleteWriteAgain() throws Exception {
    String indexDirPath = getDirPath() + 1;
    dir = new File(indexDirPath);
    dir.mkdirs(); 
    dir.deleteOnExit();
    compositeActivityValues = CompositeActivityValues.readFromFile(indexDirPath, java.util.Arrays.asList("likes"), Collections.EMPTY_LIST, ZoieConfig.DEFAULT_VERSION_COMPARATOR);
    final int valueCount = 10000;   
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
    compositeActivityValues.flushDeletes();
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
    assertEquals(1, compositeActivityValues.getValueByUID(UID_BASE + 2, "likes"));    
    
    compositeActivityValues.flush();
    Thread.sleep(1000L);
    compositeActivityValues.close();
    compositeActivityValues = CompositeActivityValues.readFromFile(indexDirPath, java.util.Arrays.asList("likes"), Collections.EMPTY_LIST, ZoieConfig.DEFAULT_VERSION_COMPARATOR);
    
    assertEquals("Found " + compositeActivityValues.metadata.count, valueCount, (int)compositeActivityValues.metadata.count);
    assertEquals(valueCount - 1, compositeActivityValues.deletedIndexes.size());
    assertEquals(1, compositeActivityValues.uidToArrayIndex.size());
   
    assertFalse(compositeActivityValues.uidToArrayIndex.containsKey(UID_BASE + 1));
    assertEquals(1, getFieldValues(compositeActivityValues)[notDeletedIndex]);
    assertEquals(1, compositeActivityValues.getValueByUID(UID_BASE + 2, "likes"));
    assertEquals((int)(valueCount * 1.5), getFieldValues(compositeActivityValues).length );
    for (int i = 0; i < valueCount; i++) {      
      compositeActivityValues.update(UID_BASE + i, String.format("%08d", valueCount * 2 + i), toMap(new JSONObject().put("likes", "+" + i)));
    }
    compositeActivityValues.flush();
    compositeActivityValues.syncWithPersistentVersion(String.format("%08d", valueCount * 3 - 1));   
   
    assertEquals(compositeActivityValues.getValueByUID(UID_BASE + 0, "likes"), 1);
    assertEquals(compositeActivityValues.getValueByUID(UID_BASE + 3, "likes"), 4);
    compositeActivityValues.close();
  }
  public void test3StartWithInconsistentIndexesAddExtraSpaceToCommentFile() throws Exception {
    String indexDirPath = getDirPath() + 2;
    dir = new File(indexDirPath);
    dir.mkdirs(); 
    dir.deleteOnExit();
    compositeActivityValues = CompositeActivityValues.readFromFile(indexDirPath, java.util.Arrays.asList("likes"), Collections.EMPTY_LIST, ZoieConfig.DEFAULT_VERSION_COMPARATOR);
    final int valueCount = 100;  
    for (int i = 0; i < valueCount; i++) {
      compositeActivityValues.update(UID_BASE + i, String.format("%08d", valueCount + i), toMap(new JSONObject().put("likes", "+1")));
    }  
    compositeActivityValues.flush();
    compositeActivityValues.syncWithPersistentVersion(String.format("%08d",  2*valueCount - 1));   
    compositeActivityValues.close();
    new File(dir, "comments.data").createNewFile();
    compositeActivityValues = CompositeActivityValues.readFromFile(indexDirPath, java.util.Arrays.asList("likes", "comments"), Collections.EMPTY_LIST, ZoieConfig.DEFAULT_VERSION_COMPARATOR);
    assertEquals(0, compositeActivityValues.getValueByUID(UID_BASE + valueCount / 2, "comments"));
    compositeActivityValues.close();
  }
  public void test4TestForUninsertedValue() throws Exception {
    String indexDirPath = getDirPath() + 3;
    dir = new File(indexDirPath);
    dir.mkdirs(); 
    dir.deleteOnExit();
    compositeActivityValues = CompositeActivityValues.readFromFile(indexDirPath, java.util.Arrays.asList("likes", "comments"), Collections.EMPTY_LIST, ZoieConfig.DEFAULT_VERSION_COMPARATOR);
    final int valueCount = 100;  
    for (int i = 0; i < valueCount; i++) {
      compositeActivityValues.update(UID_BASE + i, String.format("%08d", valueCount + i), toMap(new JSONObject().put("likes", "+1")));
    }  
    assertEquals(0, compositeActivityValues.getValueByUID(UID_BASE + valueCount / 2, "comments"));
    compositeActivityValues.flush();
    compositeActivityValues.syncWithPersistentVersion(String.format("%08d",  2*valueCount - 1));   
    compositeActivityValues.close();    
  }
  public void test5TrimMetadata() throws Exception {
    String indexDirPath = getDirPath() + 4;
    dir = new File(indexDirPath);
    dir.mkdirs(); 
    dir.deleteOnExit();
    compositeActivityValues = CompositeActivityValues.readFromFile(indexDirPath, java.util.Arrays.asList("likes"), Collections.EMPTY_LIST, ZoieConfig.DEFAULT_VERSION_COMPARATOR);
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
    compositeActivityValues = CompositeActivityValues.readFromFile(indexDirPath, java.util.Arrays.asList("likes", "comments"), Collections.EMPTY_LIST, ZoieConfig.DEFAULT_VERSION_COMPARATOR);
    assertEquals(1, compositeActivityValues.getValueByUID(UID_BASE + 1, "likes"));
    assertEquals(2, compositeActivityValues.uidToArrayIndex.size());
    assertEquals(0, compositeActivityValues.deletedIndexes.size());
    assertEquals(2, compositeActivityValues.metadata.count);
    for (int i = 0; i < valueCount; i++) {
      compositeActivityValues.update(UID_BASE + i, String.format("%08d", 2*valueCount + i), toMap(new JSONObject().put("likes", "+1")));
    }  
    assertEquals(2, compositeActivityValues.getValueByUID(UID_BASE , "likes"));
    assertEquals(2, compositeActivityValues.getValueByUID(UID_BASE + 1, "likes"));
    assertEquals(1, compositeActivityValues.getValueByUID(UID_BASE + valueCount / 2, "likes"));   
    assertEquals(valueCount, compositeActivityValues.uidToArrayIndex.size());
    assertEquals(0, compositeActivityValues.deletedIndexes.size());
    compositeActivityValues.flush();
    compositeActivityValues.syncWithPersistentVersion(String.format("%08d",  3*valueCount - 1));   
    assertEquals(valueCount, compositeActivityValues.metadata.count);
    compositeActivityValues.close();
  }
}


