package com.senseidb.indexing.activity;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.longs.Long2IntMap;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.log4j.Logger;
import org.json.JSONObject;

import proj.zoie.api.ZoieIndexReader;

import com.senseidb.indexing.activity.CompositeActivityManager.TimeAggregateInfo;
import com.senseidb.indexing.activity.CompositeActivityStorage.Update;
import com.senseidb.indexing.activity.time.TimeAggregatedActivityValues;
import com.senseidb.metrics.MetricsConstants;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.MetricName;

/**
 * 
 * Maintains the set of activityValues. The main responsibility of this class is to keep track of uid to array index mapping,
 *  persisted and in memory versions. The the document gets into the system, the class will find/create uid to index mapping, and change the activity values 
 *  for the activity fields found in the document
 *
 */
public class CompositeActivityValues {
  private static final int NUM_LOCKS = 1024;
  private static final int DEFAULT_INITIAL_CAPACITY = 5000;
  private final static Logger logger = Logger.getLogger(CompositeActivityValues.class);
  private Comparator<String> versionComparator; 
  private volatile UpdateBatch<Update> pendingDeletes = new UpdateBatch<Update>();
  protected Map<String, ActivityValues> intValuesMap = new ConcurrentHashMap<String, ActivityValues>();
  protected volatile String lastVersion = "";  
  protected Long2IntMap uidToArrayIndex = new Long2IntOpenHashMap();  
  private ReadWriteLock[] locks;
  private ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
  protected IntList deletedIndexes = new IntArrayList(2000);
  protected CompositeActivityStorage activityStorage;
  protected UpdateBatch<Update> updateBatch = new UpdateBatch<Update>(); 
  protected volatile Metadata metadata;
  private volatile boolean closed;
  private Counter deletedDocumentsCounter;
  private Counter inserteddDocumentsCounter; 
  protected CompositeActivityValues() {
   
  }
  public void init() {
    init(DEFAULT_INITIAL_CAPACITY);   
  }

  public void init(int count) {
    uidToArrayIndex = new Long2IntOpenHashMap(count);
    locks = new ReadWriteLock[NUM_LOCKS];
    for (int i = 0; i < 1024; i++) {
      locks[i] = new ReentrantReadWriteLock();
    }
    deletedDocumentsCounter = Metrics.newCounter(new MetricName(MetricsConstants.Domain,"deletedDocs","deletedDocs" ,"CompositeActivityStorage"));
    inserteddDocumentsCounter = Metrics.newCounter(new MetricName(MetricsConstants.Domain,"insertedDocs","insertedDocs" ,"CompositeActivityStorage"));
  }
  public ReadWriteLock getLock(long uid) {
    return locks[(int) (uid % locks.length)];
  }
  public void updateVersion(String version) {
    if (versionComparator.compare(lastVersion, version) < 0) {
      lastVersion = version;
    }
  }
  public void update(long uid, final String version, Map<String, Object> map) {
    if (intValuesMap.isEmpty()) {
      return;
    }
    if (versionComparator.compare(lastVersion, version) >= 0) {
      return;
    }
    if (map.isEmpty()) {
      lastVersion = version;
      return;
    }
    int index = -1;
    ReadWriteLock lock = getLock(uid);
    Lock writeLock = lock.writeLock();
    boolean needToFlush = false;
    try {
      writeLock.lock();      
      if (uidToArrayIndex.containsKey(uid)) {
        index = uidToArrayIndex.get(uid);       
      } else {
        boolean deletedDocsPresent = false;
        synchronized (deletedIndexes) {
          if (deletedIndexes.size() > 0) {
             index = deletedIndexes.removeInt(deletedIndexes.size() - 1);
             uidToArrayIndex.put(uid, index); 
             deletedDocsPresent = true;           
          }
        }
        if (!deletedDocsPresent) {          
          index = uidToArrayIndex.size();
          uidToArrayIndex.put(uid, index);
        }
        needToFlush = updateBatch.addFieldUpdate(new Update(index, uid));
      }      
      // System.out.println("update uid = " + uid + ", index = " + index + ", threadID = " + Thread.currentThread().getId());
      needToFlush = needToFlush || updateActivities(map, index);
      lastVersion = version;
    } finally {
      writeLock.unlock();
    }   
    if (needToFlush) {
      flush();
    }
  }
  public ActivityIntValues getActivityIntValues(String fieldName) {
    ActivityValues activityValues = intValuesMap.get(fieldName);
    if (activityValues == null) {
      if (fieldName.contains(":")) {
        return ((TimeAggregatedActivityValues)intValuesMap.get(fieldName.substring(0, fieldName.indexOf(":")))).getValuesMap().get(fieldName.substring(fieldName.indexOf(":") + 1));
      }
      return null;
    } else if (activityValues instanceof ActivityIntValues) {
      return (ActivityIntValues)  activityValues;
    } else {
      return ((TimeAggregatedActivityValues)  activityValues).getDefaultIntValues();
    }
  }

  private boolean updateActivities(Map<String, Object> map, int index) {
    boolean needToFlush = false;
    for (ActivityValues activityIntValues : intValuesMap.values()) {
      Object value = map.get(activityIntValues.getFieldName());
      if (value != null) {
        needToFlush = needToFlush | activityIntValues.update(index, value);
      }
    }
    return needToFlush;
  }
  
  /**
   * Tells whether the document will modify activity values
   * @param event
   * @return
   */
  private boolean matchesFields(JSONObject event) {
    boolean matchedEvent = false;
    for (String field : intValuesMap.keySet()) {
      if (event.opt(field) != null) {
        matchedEvent = true;
      }
    }
    return matchedEvent;
  }

  /**Deletes documents from the activity engine
   * @param uids
   */
  public void delete(long... uids) {
    boolean needToFlush = false;
    for (long uid : uids) {
      if (uid == Long.MIN_VALUE) {
        continue;
      }
      ReadWriteLock lock = getLock(uid);
      Lock writeLock = lock.writeLock();
      try {
        writeLock.lock();
        if (!uidToArrayIndex.containsKey(uid)) {
          continue;
        }
        int index = uidToArrayIndex.remove(uid);
        for (ActivityValues activityIntValues :  intValuesMap.values()) {
          activityIntValues.delete(index);
        }
        needToFlush = needToFlush || pendingDeletes.addFieldUpdate(new Update(index, Long.MIN_VALUE));
      } finally {
        writeLock.unlock();
      }
    }
    if (needToFlush) {
      flushDeletes();
    }
  }
  /**
   * Propagates the deletes to disk. After calling this method freed array indexes can be reused for different document uids
   */
  public void flushDeletes() {
    if (pendingDeletes.updates.isEmpty()) {
      return;
    }
    final UpdateBatch<Update> deleteBatch = pendingDeletes;
    pendingDeletes = new UpdateBatch<Update>();
    
    executor.submit(new Runnable() {
      @Override
      public void run() {
        if (closed) {
          return;
        }
        Collections.reverse(deleteBatch.updates);
        activityStorage.flush(deleteBatch.updates);
        synchronized (deletedIndexes) {
          
          for (Update update : deleteBatch.updates) {
            deletedIndexes.add(update.index);
          }       
        }
      }
    });
  }
  
  public void syncWithPersistentVersion(String version) {
    synchronized (this) {
      while (versionComparator.compare(metadata.version, version) < 0) {
        try {
          this.wait(400L);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    }
  }
  public void syncWithVersion(String version) {
    synchronized (this) {
      while (versionComparator.compare(lastVersion, version) < 0) {
        try {
          this.wait(400L);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    }
  }
  public String getVersion() {
    return lastVersion;
  }
  /**
   * flushes pening updates to disk
   */
  public synchronized void flush() {
    if (closed) {
      return;
    }
    final UpdateBatch<Update> oldBatch = updateBatch;
    updateBatch = new UpdateBatch<CompositeActivityStorage.Update>();    
    final List<Runnable> underlyingFlushes = new ArrayList<Runnable>(intValuesMap.size());
    for (ActivityValues activityIntValues :  intValuesMap.values()) {
      underlyingFlushes.add(activityIntValues.prepareFlush());
    }
    final String version = lastVersion;
    final int count;
    synchronized (deletedIndexes) {
      count = uidToArrayIndex.size() + deletedIndexes.size();
      inserteddDocumentsCounter.clear();
      inserteddDocumentsCounter.inc(uidToArrayIndex.size());
      deletedDocumentsCounter.clear();
      deletedDocumentsCounter.inc( uidToArrayIndex.size());
      logger.info("Flush compositeActivityValues. Documents = " +  uidToArrayIndex.size() + ", Deletes = " + deletedIndexes.size());
    }
     executor.submit(new Runnable() {      
      @Override
      public void run() {
        if (closed) {
          return;
        }
        activityStorage.flush(oldBatch.updates);
        for (Runnable runnable : underlyingFlushes) {
          runnable.run();
        }
        metadata.update(version, count);
      }
    });   
     flushDeletes();
  }
  
  public void close() {
    closed = true;    
    activityStorage.close();
    for (ActivityValues activityIntValues :  intValuesMap.values()) {
      activityIntValues.close();
    }
  }
  /**
   * A factory method that constructs the CompositeActivityValues
   * @param indexDirPath
   * @param fieldNames
   * @param aggregatedActivities
   * @param versionComparator
   * @return
   */
  public static CompositeActivityValues readFromFile(String indexDirPath, List<String> fieldNames, List<TimeAggregateInfo> aggregatedActivities, Comparator<String> versionComparator) {    
    CompositeActivityStorage persistentColumnManager = new CompositeActivityStorage(indexDirPath);
    persistentColumnManager.init();
    Metadata metadata = new Metadata(indexDirPath);
    metadata.init();
    CompositeActivityValues ret = persistentColumnManager.getActivityDataFromFile(metadata);
    ret.deletedDocumentsCounter.inc(ret.deletedIndexes.size());
    ret.inserteddDocumentsCounter.inc(ret.uidToArrayIndex.size());
    logger.info("Init compositeActivityValues. Documents = " +  ret.uidToArrayIndex.size() + ", Deletes = " +ret.deletedIndexes.size());
    ret.metadata = metadata;
    ret.versionComparator = versionComparator;
    ret.lastVersion = metadata.version;
    ret.intValuesMap = new HashMap<String, ActivityValues>(fieldNames.size());
    for (TimeAggregateInfo aggregatedActivity : aggregatedActivities) {
      ret.intValuesMap.put(aggregatedActivity.fieldName, TimeAggregatedActivityValues.valueOf(aggregatedActivity.fieldName, aggregatedActivity.times, 
          metadata.count, indexDirPath));
    }
    for (String field : fieldNames) {
      if (!ret.intValuesMap.containsKey(field)) {
        ret.intValuesMap.put(field, ActivityIntValues.readFromFile(indexDirPath, field, metadata.count));
      }
    }    
    return ret;
  }
  public int[] precomputeArrayIndexes(long[] uids) {    
    int[] ret = new int[uids.length];   
    for (int i = 0; i < uids.length; i++) {
      long uid = uids[i];
      if (uid == ZoieIndexReader.DELETED_UID) {
        ret[i] = -1;
        continue;
      }
      Lock lock = getLock(uid).readLock();
      try {
        lock.lock();
        if (!uidToArrayIndex.containsKey(uid)) {
          ret[i] = -1;
        } else {
          ret[i] = uidToArrayIndex.get(uid);       
        }
      } finally {
        lock.unlock();
      }
    }
    return ret;
  }
  public Map<String, ActivityValues> getActivityValuesMap() {
    return intValuesMap;
  }
  public int getValueByUID(long uid, String column) {
    if (!uidToArrayIndex.containsKey(uid)) {
     return Integer.MIN_VALUE;
    }
    Lock lock = getLock(uid).readLock();
    try {
      lock.lock();
    return getActivityIntValues(column).getFieldValues()[uidToArrayIndex.get(uid)];
    } finally {
      lock.unlock();
    }
    }
  
  
}
