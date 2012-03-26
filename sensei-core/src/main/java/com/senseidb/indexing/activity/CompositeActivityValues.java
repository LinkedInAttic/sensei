package com.senseidb.indexing.activity;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.longs.Long2IntMap;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;

import java.util.ArrayList;
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

import org.json.JSONObject;
import org.springframework.util.Assert;

import proj.zoie.api.ZoieIndexReader;

import com.senseidb.indexing.activity.CompositeActivityManager.TimeAggregateInfo;
import com.senseidb.indexing.activity.CompositeActivityStorage.Update;
import com.senseidb.indexing.activity.time.TimeAggregatedActivityValues;

public class CompositeActivityValues {
  private static final String AGGREGATED_SUFFIX = "_aggregated";
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
  protected CompositeActivityValues() {
   
  }
  public void init() {
    init(5000);   
  }

  public void init(int count) {
    uidToArrayIndex = new Long2IntOpenHashMap(count);
    locks = new ReadWriteLock[1024];
    for (int i = 0; i < 1024; i++) {
      locks[i] = new ReentrantReadWriteLock();
    }
  }
  public ReadWriteLock getLock(long uid) {
    return locks[(int) (uid % locks.length)];
  }
  public void updateVersion(String version) {
    if (versionComparator.compare(lastVersion, version) < 0) {
      lastVersion = version;
    }
  }
  public void update(long uid, final String version, JSONObject event) {
    if (intValuesMap.isEmpty()) {
      return;
    }
    if (versionComparator.compare(lastVersion, version) > 0) {
      return;
    }
    if (!matchesFields(event)) {
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
             uidToArrayIndex.put(uid, index);             ;
             deletedDocsPresent = true;
          }
        }
        if (!deletedDocsPresent) {          
          index = uidToArrayIndex.size();
          uidToArrayIndex.put(uid, index);
        }
        needToFlush = updateBatch.addFieldUpdate(new Update(index, uid));
      }      
      needToFlush = needToFlush || updateActivities(event, index);
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
      return null;
    } else if (activityValues instanceof ActivityIntValues) {
      return (ActivityIntValues)  activityValues;
    } else {
      return ((TimeAggregatedActivityValues)  activityValues).getDefaultIntValues();
    }
  }
private boolean updateActivities(JSONObject event, int index) {
	boolean needToFlush = false;
	for (ActivityValues activityIntValues :  intValuesMap.values()) {
        Object value = event.opt(activityIntValues.getFieldName());
        if (value != null) {
        	needToFlush = needToFlush | activityIntValues.update(index, value);
        }
      }
	return needToFlush;
}
  private boolean matchesFields(JSONObject event) {
    boolean matchedEvent = false;
    for (String field : intValuesMap.keySet()) {
      if (event.opt(field) != null) {
        matchedEvent = true;
      }
    }
    return matchedEvent;
  }

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
  public String getVersion() {
    return lastVersion;
  }
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
  public static CompositeActivityValues readFromFile(String indexDirPath, List<String> fieldNames, List<TimeAggregateInfo> aggregatedActivities, Comparator<String> versionComparator) {    
    CompositeActivityStorage persistentColumnManager = new CompositeActivityStorage(indexDirPath);
    persistentColumnManager.init();
    Metadata metadata = new Metadata(indexDirPath);
    metadata.init();
    CompositeActivityValues ret = persistentColumnManager.getActivityDataFromFile(metadata);
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
          throw new IllegalStateException("Couldn't find the field value for the uid = " + uid);
        }
        ret[i] = uidToArrayIndex.get(uid);       
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
    Lock lock = getLock(uid).readLock();
    try {
      lock.lock();
    return getActivityIntValues(column).fieldValues[uidToArrayIndex.get(uid)];
    } finally {
      lock.unlock();
    }
    }
  
  
}
