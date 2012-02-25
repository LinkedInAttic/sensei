package com.senseidb.indexing.activity;

import java.io.File;

import junit.framework.TestCase;

import org.junit.Before;
import org.junit.Test;

import proj.zoie.impl.indexing.ZoieConfig;

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
  
  public void test1WriteValuesAndReadJustAfterThat() {
    FileStorage activityFieldStore = new FileStorage("field", getDirPath()); 
    activityFieldStore.init();
    ActivityFieldValues activityData = activityFieldStore.getActivityDataFromFile(ZoieConfig.DEFAULT_VERSION_COMPARATOR);
    int valueCount = 100000;
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
}


