package com.senseidb.indexing.activity;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import com.senseidb.conf.SenseiSchema;
import com.senseidb.indexing.activity.primitives.ActivityPrimitivesStorage;
import com.senseidb.indexing.activity.time.TimeAggregatedActivityValues;

public class ActivityPersistenceFactory {
  private static Logger logger = Logger.getLogger(ActivityPersistenceFactory.class);
 
  private static ThreadLocal<ActivityPersistenceFactory> overrideForCurrentThread = new ThreadLocal<ActivityPersistenceFactory>();
 

  private Metadata metadata;
  private static String indexDirPath;


  private final ActivityConfig activityConfig;
  public static ActivityPersistenceFactory getInstance(String indexDirPath) {
    return getInstance(indexDirPath, new ActivityConfig());
  }
  public static ActivityPersistenceFactory getInstance(String indexDirPath, ActivityConfig activityConfig) {

    if (overrideForCurrentThread.get() != null) {
      ActivityPersistenceFactory ret = overrideForCurrentThread.get();     
      return ret;
    }    

    return new ActivityPersistenceFactory( indexDirPath, activityConfig);   

  }
  public static ActivityPersistenceFactory getInMemoryInstance() {
    return new ActivityInMemoryFactory();
  }


  protected ActivityPersistenceFactory(String indexDirPath, ActivityConfig activityConfig) {
    this.indexDirPath = indexDirPath;
    this.activityConfig = activityConfig;

  }
  

  protected CompositeActivityStorage getCompositeStorage() {
    CompositeActivityStorage ret = new CompositeActivityStorage(indexDirPath);
    ret.init();
    return ret;
  }
  
 

  public ActivityPrimitivesStorage getActivivityPrimitivesStorage(String fieldName) {
    ActivityPrimitivesStorage activityPrimitivesStorage = new ActivityPrimitivesStorage(fieldName, indexDirPath);
    activityPrimitivesStorage.init();
    return activityPrimitivesStorage;
  }
  
  public AggregatesMetadata createAggregatesMetadata(String fieldName) {
    AggregatesMetadata ret = new AggregatesMetadata();
    File aggregatesFile = new File(indexDirPath, fieldName + ".aggregates");
    try {
    if (!aggregatesFile.exists()) {
      aggregatesFile.createNewFile();
      //minimum possible time
      ret.lastUpdatedTime = 0;
      FileUtils.writeStringToFile(aggregatesFile, String.valueOf(ret.lastUpdatedTime));
    } else {
      ret.lastUpdatedTime = Integer.parseInt(FileUtils.readFileToString(aggregatesFile));
    }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    ret.aggregatesFile = aggregatesFile;
    return ret;
  } 
  
  public static class AggregatesMetadata {
    protected int lastUpdatedTime;
    protected File aggregatesFile;
    protected AggregatesMetadata() {      
     
    }
   
    public void updateTime(int currentTime) {
      lastUpdatedTime  = currentTime;
      try {
        FileUtils.writeStringToFile(aggregatesFile, String.valueOf(currentTime));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    public int getLastUpdatedTime() {
      return lastUpdatedTime;
    }
    
  }
  
  public Metadata getMetadata() {
    if (metadata == null) {
      metadata = new Metadata(ActivityPersistenceFactory.this.indexDirPath);
    }
    return metadata;
  }
  public static void setOverrideForCurrentThread(ActivityPersistenceFactory overrideForCurrentThread) {
    ActivityPersistenceFactory.overrideForCurrentThread.set(overrideForCurrentThread);
  }
  public ActivityConfig getActivityConfig() {
    return activityConfig;
  }
  
}
