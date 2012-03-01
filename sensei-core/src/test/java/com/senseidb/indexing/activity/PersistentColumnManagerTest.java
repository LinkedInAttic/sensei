package com.senseidb.indexing.activity;

import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;

import java.io.File;

import junit.framework.TestCase;

import org.junit.Before;
import org.junit.Test;

import proj.zoie.impl.indexing.ZoieConfig;
import scala.actors.threadpool.Arrays;

import com.senseidb.test.SenseiStarter;

public class PersistentColumnManagerTest extends TestCase {
  private File dir; 
  
  
  public void setUp() {
    String pathname = getDirPath();
    SenseiStarter.rmrf(new File(pathname));
    dir = new File(pathname);
    dir.mkdirs();
    
  }
  private String getDirPath() {
    return "sensei-test/activity";
  }
  @Override
  protected void tearDown() throws Exception {
    SenseiStarter.rmrf(new File("sensei-test"));
  }
  public void test1WriteValuesAndReadJustAfterThat() {
    FileStorage activityFieldStore = new FileStorage("field", getDirPath()); 
    activityFieldStore.init();
    ActivityFieldValues activityData = activityFieldStore.getActivityDataFromFile(ZoieConfig.DEFAULT_VERSION_COMPARATOR);
    int valueCount = 10000;
    for (int i = 0; i < valueCount; i++) { 
      activityData.update(10000000000L + i, String.format("%08d", i), "+1");
    }    
    activityData.syncWithPersistentVersion(String.format("%08d", valueCount - 1));
    activityFieldStore.close();
    activityFieldStore = new FileStorage("field", getDirPath()); 
    activityFieldStore.init();
    activityData = activityFieldStore.getActivityDataFromFile(ZoieConfig.DEFAULT_VERSION_COMPARATOR);
    assertEquals("Found " + activityData.uidToArrayIndex.size(), valueCount, activityData.uidToArrayIndex.size());
    assertEquals((int)(valueCount * 1.5), activityData.fieldValues.length );
    for (int i = 0; i < valueCount; i++) {      
      activityData.update(10000000000L + i, String.format("%08d", valueCount + i), "+" + i);
    }
    activityData.syncWithPersistentVersion(String.format("%08d", valueCount * 2 - 1));
    activityFieldStore.close();
    assertEquals(activityData.fieldValues[0], 1);
    assertEquals(activityData.fieldValues[3], 4);
    activityData = activityFieldStore.getActivityDataFromFile(ZoieConfig.DEFAULT_VERSION_COMPARATOR);
    assertEquals(activityData.fieldValues[0], 1);
    assertEquals(activityData.fieldValues[3], 4);
  }
  public void test2WriteDeleteWriteAgain() {
    FileStorage activityFieldStore = new FileStorage("field", getDirPath()); 
    activityFieldStore.init();
     ActivityFieldValues activityData = activityFieldStore.getActivityDataFromFile(ZoieConfig.DEFAULT_VERSION_COMPARATOR);
    final int valueCount = 10000;
    long UID_BASE = 10000000000L;
    for (int i = 0; i < valueCount; i++) {
      activityData.update(UID_BASE + i, String.format("%08d", i), "+1");
    }  
    activityData.syncWithPersistentVersion(String.format("%08d", valueCount - 1));
    LongList uidsToDelete = new LongArrayList();
    for (int i = 0; i < valueCount; i++) {
      if (i == 2) {
        continue;
      }
      uidsToDelete.add(UID_BASE + i);
      if (i %1000 == 0) {
        activityData.delete(uidsToDelete);
        uidsToDelete.clear();
      } 
    }
    activityData.delete(uidsToDelete);
    int notDeletedIndex = activityData.uidToArrayIndex.get(UID_BASE + 2);
    final ActivityFieldValues testActivityData = activityData;    
    Wait.until(10000L, "", new Wait.Condition() {      
      public boolean evaluate() {
        synchronized (testActivityData.deletedIndexes) {
          return testActivityData.deletedIndexes.size() == valueCount - 1;
        }
      }
    });
    assertEquals(valueCount - 1, activityData.deletedIndexes.size());
    assertEquals(1, activityData.uidToArrayIndex.size());
    assertEquals(Integer.MIN_VALUE, activityData.fieldValues[notDeletedIndex + 1]);
    assertEquals(1, activityData.fieldValues[notDeletedIndex]);
    assertEquals(1, activityData.getValueByUID(UID_BASE + 2));    
    activityFieldStore.close();
    activityFieldStore = new FileStorage("field", getDirPath()); 
    activityFieldStore.init();
    activityData = activityFieldStore.getActivityDataFromFile(ZoieConfig.DEFAULT_VERSION_COMPARATOR);
    assertEquals("Found " + activityData.arraySize, valueCount, activityData.arraySize);
    assertEquals(valueCount - 1, activityData.deletedIndexes.size());
    assertEquals(1, activityData.uidToArrayIndex.size());
   
    assertEquals(Integer.MIN_VALUE, activityData.fieldValues[notDeletedIndex + 1]);
    assertEquals(1, activityData.fieldValues[notDeletedIndex]);
    assertEquals(1, activityData.getValueByUID(UID_BASE + 2));
    assertEquals((int)(valueCount * 1.5), activityData.fieldValues.length );
    for (int i = 0; i < valueCount; i++) {      
      activityData.update(UID_BASE + i, String.format("%08d", valueCount + i), "+" + i);
    }
    activityData.syncWithPersistentVersion(String.format("%08d", valueCount * 2 - 1));   
    System.out.println(Arrays.toString(activityData.fieldValues));
    assertEquals(activityData.getValueByUID(UID_BASE + 0), 0);
    assertEquals(activityData.getValueByUID(UID_BASE + 3), 3);
    activityData.close();
  }
}


