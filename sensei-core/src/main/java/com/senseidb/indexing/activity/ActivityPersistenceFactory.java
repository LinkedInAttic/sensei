package com.senseidb.indexing.activity;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import com.senseidb.indexing.activity.CompositeActivityManager.TimeAggregateInfo;
import com.senseidb.indexing.activity.time.TimeAggregatedActivityValues;

public class ActivityPersistenceFactory {
  private static Logger logger = Logger.getLogger(ActivityPersistenceFactory.class);
  private static ActivityPersistenceFactory instance = new ActivityPersistenceFactory();
  private static ThreadLocal<ActivityPersistenceFactory> overrideForCurrentThread = new ThreadLocal<ActivityPersistenceFactory>();
  public static ActivityPersistenceFactory getInstance() {
    if (overrideForCurrentThread.get() != null) {
      ActivityPersistenceFactory ret = overrideForCurrentThread.get();     
      return ret;
    }
    return instance;
  }
  public static ActivityPersistenceFactory getInMemoryInstance() {
    return new ActivityInMemoryFactory();
  }
  protected ActivityPersistenceFactory() {
    // TODO Auto-generated constructor stub
  }
  /**
   * A factory method that constructs the CompositeActivityValues
   * @param indexDirPath
   * @param fieldNames
   * @param aggregatedActivities
   * @param versionComparator
   * @return
   */
  public  CompositeActivityValues createCompositeValues(String indexDirPath, List<String> fieldNames, List<TimeAggregateInfo> aggregatedActivities, Comparator<String> versionComparator) {    
    CompositeActivityStorage persistentColumnManager = getCompositeStorage(indexDirPath);
    persistentColumnManager.init();
    Metadata metadata = createMetadata(indexDirPath);
    metadata.init();
    CompositeActivityValues ret = persistentColumnManager.getActivityDataFromFile(metadata);
    ret.reclaimedDocumentsCounter.inc(ret.deletedIndexes.size());
    ret.currentDocumentsCounter.inc(ret.uidToArrayIndex.size());
    logger.info("Init compositeActivityValues. Documents = " +  ret.uidToArrayIndex.size() + ", Deletes = " +ret.deletedIndexes.size());
    ret.metadata = metadata;
    ret.versionComparator = versionComparator;
    ret.lastVersion = metadata.version;
    ret.intValuesMap = new HashMap<String, ActivityValues>(fieldNames.size());
    for (TimeAggregateInfo aggregatedActivity : aggregatedActivities) {
      ret.intValuesMap.put(aggregatedActivity.fieldName, createTimeAggregatedValues(aggregatedActivity.fieldName, aggregatedActivity.times, 
          metadata.count, indexDirPath));
    }
    for (String field : fieldNames) {
      if (!ret.intValuesMap.containsKey(field)) {
        ret.intValuesMap.put(field, createIntValues(indexDirPath, field, metadata.count));
      }
    }    
    return ret;
  }

  protected CompositeActivityStorage getCompositeStorage(String indexDirPath) {
    return new CompositeActivityStorage(indexDirPath);
  }
  
  public ActivityIntValues createIntValues(String indexDirPath,
      String fieldName, int count) {
    ActivityIntStorage persistentColumnManager = getActivivityIntStorage(indexDirPath, fieldName);
    persistentColumnManager.init();
    return persistentColumnManager.getActivityDataFromFile(count);
  }

  protected ActivityIntStorage getActivivityIntStorage(String indexDirPath, String fieldName) {
    return new ActivityIntStorage(fieldName, indexDirPath);
  }
  public TimeAggregatedActivityValues createTimeAggregatedValues(String fieldName, List<String> times, int count, String indexDirPath) {
    TimeAggregatedActivityValues ret = new TimeAggregatedActivityValues(fieldName, times, count, indexDirPath, this);
    ret.init(count > 0 ? count : 15000);
    return ret;
  }
  public AggregatesMetadata createAggregatesMetadata(String dirPath, String fieldName) {
    AggregatesMetadata ret = new AggregatesMetadata();
    File aggregatesFile = new File(dirPath, fieldName + ".aggregates");
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
  
  public Metadata createMetadata(String indexDir) {
    return new Metadata(indexDir);
  }
  public static void setOverrideForCurrentThread(ActivityPersistenceFactory overrideForCurrentThread) {
    ActivityPersistenceFactory.overrideForCurrentThread.set(overrideForCurrentThread);
  }
}
