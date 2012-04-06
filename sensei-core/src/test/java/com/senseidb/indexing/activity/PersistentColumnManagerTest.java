package com.senseidb.indexing.activity;

import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;

import java.io.File;
import java.util.Collections;

import junit.framework.TestCase;

import org.json.JSONObject;

import proj.zoie.impl.indexing.ZoieConfig;
import scala.actors.threadpool.Arrays;

import com.senseidb.test.SenseiStarter;

public class PersistentColumnManagerTest extends TestCase {
  private static final long UID_BASE = 10000000000L;
  private File dir; 
  
  
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
    CompositeActivityValues compositeActivityValues = CompositeActivityValues.readFromFile(getDirPath(), java.util.Arrays.asList("likes"), Collections.EMPTY_LIST, ZoieConfig.DEFAULT_VERSION_COMPARATOR);
  
    int valueCount = 10000;
    for (int i = 0; i < valueCount; i++) { 
      compositeActivityValues.update(10000000000L + i, String.format("%08d", i), new JSONObject().put("likes", "+1"));
    }    
    compositeActivityValues.syncWithPersistentVersion(String.format("%08d", valueCount - 1));
    compositeActivityValues.close();
    compositeActivityValues = CompositeActivityValues.readFromFile(getDirPath(), java.util.Arrays.asList("likes"), Collections.EMPTY_LIST, ZoieConfig.DEFAULT_VERSION_COMPARATOR);
    
    assertEquals("Found " + compositeActivityValues.uidToArrayIndex.size(), valueCount, compositeActivityValues.uidToArrayIndex.size());
    assertEquals((int)(valueCount * 1.5), getFieldValues(compositeActivityValues).length );
    for (int i = 0; i < valueCount; i++) {      
      compositeActivityValues.update(10000000000L + i, String.format("%08d", valueCount + i), new JSONObject().put("likes","+" + i));
    }
    compositeActivityValues.syncWithPersistentVersion(String.format("%08d", valueCount * 2 - 1));
    compositeActivityValues.close();
    assertEquals(getFieldValues(compositeActivityValues)[0], 1);
    assertEquals(getFieldValues(compositeActivityValues)[3], 4);
    compositeActivityValues = CompositeActivityValues.readFromFile(getDirPath(), java.util.Arrays.asList("likes"), Collections.EMPTY_LIST, ZoieConfig.DEFAULT_VERSION_COMPARATOR);
    assertEquals(getFieldValues(compositeActivityValues)[0], 1);
    assertEquals(getFieldValues(compositeActivityValues)[3], 4);
    compositeActivityValues.close();
  }
private int[] getFieldValues(CompositeActivityValues compositeActivityValues){
	return ((ActivityIntValues)compositeActivityValues.intValuesMap.get("likes")).fieldValues;
	}
  public void test2WriteDeleteWriteAgain() throws Exception {
    String indexDirPath = getDirPath() + 1;
    dir = new File(indexDirPath);
    dir.mkdirs(); 
    dir.deleteOnExit();
    CompositeActivityValues compositeActivityValues = CompositeActivityValues.readFromFile(indexDirPath, java.util.Arrays.asList("likes"), Collections.EMPTY_LIST, ZoieConfig.DEFAULT_VERSION_COMPARATOR);
    final int valueCount = 10000;   
    for (int i = 0; i < valueCount; i++) {
      compositeActivityValues.update(UID_BASE + i, String.format("%08d", valueCount + i), new JSONObject().put("likes", "+1"));
    }  
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
    compositeActivityValues.delete(uidsToDelete.toLongArray());
    int notDeletedIndex = compositeActivityValues.uidToArrayIndex.get(UID_BASE + 2);
    final CompositeActivityValues testActivityData = compositeActivityValues;
    testActivityData.flushDeletes();
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
    compositeActivityValues.flushDeletes();
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
      compositeActivityValues.update(UID_BASE + i, String.format("%08d", valueCount * 2 + i), new JSONObject().put("likes", "+" + i));
    }
    compositeActivityValues.syncWithPersistentVersion(String.format("%08d", valueCount * 2 - 1));   
   
    assertEquals(compositeActivityValues.getValueByUID(UID_BASE + 0, "likes"), 0);
    assertEquals(compositeActivityValues.getValueByUID(UID_BASE + 3, "likes"), 4);
    compositeActivityValues.close();
  }
}


