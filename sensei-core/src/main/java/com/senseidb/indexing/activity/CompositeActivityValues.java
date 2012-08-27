/**
 * This software is licensed to you under the Apache License, Version 2.0 (the
 * "Apache License").
 *
 * LinkedIn's contributions are made under the Apache License. If you contribute
 * to the Software, the contributions will be deemed to have been made under the
 * Apache License, unless you expressly indicate otherwise. Please do not make any
 * contributions that would be inconsistent with the Apache License.
 *
 * You may obtain a copy of the Apache License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, this software
 * distributed under the Apache License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the Apache
 * License for the specific language governing permissions and limitations for the
 * software governed under the Apache License.
 *
 * © 2012 LinkedIn Corp. All Rights Reserved.  
 */
package com.senseidb.indexing.activity;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.longs.Long2IntMap;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;

import java.util.ArrayList;
import java.util.Collection;
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

import proj.zoie.api.ZoieIndexReader;

import com.senseidb.conf.SenseiSchema;
import com.senseidb.indexing.activity.CompositeActivityManager.TimeAggregateInfo;
import com.senseidb.indexing.activity.CompositeActivityStorage.Update;
import com.senseidb.indexing.activity.primitives.ActivityFloatValues;
import com.senseidb.indexing.activity.primitives.ActivityIntValues;
import com.senseidb.indexing.activity.primitives.ActivityPrimitiveValues;
import com.senseidb.indexing.activity.time.TimeAggregatedActivityValues;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.MetricName;

/**
 * 
 *  Maintains the set of activityValues. The main responsibility of this class is to keep track of uid to array index mapping,
 *  persisted and in memory versions. The the document gets into the system, the class will find/create uid to index mapping, and change the activity values 
 *  for the activity fields found in the document
 *
 */
public class CompositeActivityValues {
  
  private static final int DEFAULT_INITIAL_CAPACITY = 5000;
  private final static Logger logger = Logger.getLogger(CompositeActivityValues.class);
  protected Comparator<String> versionComparator; 
  private volatile UpdateBatch<Update> pendingDeletes;
  protected Map<String, ActivityValues> valuesMap = new ConcurrentHashMap<String, ActivityValues>();
  protected volatile String lastVersion = "";  
  protected Long2IntMap uidToArrayIndex = new Long2IntOpenHashMap();  
  protected ReadWriteLock globalLock = new ReentrantReadWriteLock();
  protected ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
  protected IntList deletedIndexes = new IntArrayList(2000);  
  protected CompositeActivityStorage activityStorage;
  protected UpdateBatch<Update> updateBatch; 
  protected RecentlyAddedUids recentlyAddedUids; 
  protected volatile Metadata metadata;
  
  private volatile boolean closed;
  private ActivityConfig activityConfig;
  
  protected static Counter reclaimedDocumentsCounter;
  protected static Counter currentDocumentsCounter;
  protected static Counter deletedDocumentsCounter;
  protected static Counter insertedDocumentsCounter;
  protected static Counter totalUpdatesCounter;
  static {
    reclaimedDocumentsCounter = Metrics.newCounter(new MetricName(CompositeActivityValues.class, "reclaimedActivityDocs"));
    currentDocumentsCounter = Metrics.newCounter(new MetricName(CompositeActivityValues.class, "currentActivityDocs"));
    deletedDocumentsCounter = Metrics.newCounter(new MetricName(CompositeActivityValues.class, "deletedActivityDocs"));
    insertedDocumentsCounter = Metrics.newCounter(new MetricName(CompositeActivityValues.class, "insertedActivityDocs"));
    totalUpdatesCounter = Metrics.newCounter(new MetricName(CompositeActivityValues.class, "totalUpdatesCounter"));
  }
  CompositeActivityValues() {    
   
  }
  public void init() {
    init(DEFAULT_INITIAL_CAPACITY);   
  }

  public void init(int count) {
    uidToArrayIndex = new Long2IntOpenHashMap(count);  
    
  }
  
  public void updateVersion(String version) {
    if (versionComparator.compare(lastVersion, version) < 0) {
      lastVersion = version;
    }
  }
  public int update(long uid, final String version, Map<String, Object> map) {
    if (valuesMap.isEmpty()) {
      return -1;
    }
    if (versionComparator.compare(lastVersion, version) > 0) {
      return -1;
    }
    if (map.isEmpty()) {
      lastVersion = version;
      return -1;
    }
    int index = -1;
    
    Lock writeLock = globalLock.writeLock();
    boolean needToFlush = false;
    try {
      writeLock.lock();      
      totalUpdatesCounter.inc();
      if (uidToArrayIndex.containsKey(uid)) {
        index = uidToArrayIndex.get(uid); 
      } else {
        insertedDocumentsCounter.inc();       
        synchronized (deletedIndexes) {
          if (deletedIndexes.size() > 0) {
             index = deletedIndexes.removeInt(deletedIndexes.size() - 1);
          } else {
            index = uidToArrayIndex.size();
          }
        } 
        uidToArrayIndex.put(uid, index); 
        recentlyAddedUids.add(uid);
        needToFlush = updateBatch.addFieldUpdate(new Update(index, uid));
      }      
      boolean currentUpdate = updateActivities(map, index);
      needToFlush = needToFlush || currentUpdate;
      lastVersion = version;
    } finally {
      writeLock.unlock();
    }   
    if (needToFlush) {
      flush();
    }
    return index;
  }
  public ActivityPrimitiveValues getActivityValues(String fieldName) {
    ActivityValues activityValues = valuesMap.get(fieldName);
    if (activityValues == null) {
      if (fieldName.contains(":")) {
        return ((TimeAggregatedActivityValues)valuesMap.get(fieldName.substring(0, fieldName.indexOf(":")))).getValuesMap().get(fieldName.substring(fieldName.indexOf(":") + 1));
      }
      return null;
    } else if (activityValues instanceof ActivityIntValues) {
      return (ActivityIntValues)  activityValues;
    } else if (activityValues instanceof ActivityFloatValues) {
      return (ActivityFloatValues)  activityValues;
    } else {
      return ((TimeAggregatedActivityValues)  activityValues).getDefaultIntValues();
    }
  }

  private boolean updateActivities(Map<String, Object> map, int index) {
    boolean needToFlush = false;
    for (ActivityValues activityIntValues : valuesMap.values()) {
      Object value = map.get(activityIntValues.getFieldName());
      if (value != null) {
        needToFlush = needToFlush | activityIntValues.update(index, value);
      } else {
        needToFlush = needToFlush | activityIntValues.update(index, 0);
      }
    }
    return needToFlush;
  }
  
  
  /**Deletes documents from the activity engine
   * @param uids
   */
  public void delete(long... uids) {
    boolean needToFlush = false;
    if (uids.length == 0) {
      return;
    }
   
    for (long uid : uids) {
      if (uid == Long.MIN_VALUE) {
        continue;
      }     
      Lock writeLock = globalLock.writeLock();
      try {
        writeLock.lock();
        if (!uidToArrayIndex.containsKey(uid)) {
          continue;
        }
        deletedDocumentsCounter.inc();
        int index = uidToArrayIndex.remove(uid);       
        for (ActivityValues activityIntValues :  valuesMap.values()) {
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
    pendingDeletes = new UpdateBatch<Update>(activityConfig);
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
      while (versionComparator.compare(metadata != null ? metadata.version : lastVersion, version) < 0) {
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
   * flushes pending updates to disk
   */
  public synchronized void flush() {
    
    if (closed) {
      return;
    }
    final UpdateBatch<Update> oldBatch = updateBatch;
    updateBatch = new UpdateBatch<CompositeActivityStorage.Update>(activityConfig);    
    final List<Runnable> underlyingFlushes = new ArrayList<Runnable>(valuesMap.size());
    for (ActivityValues activityIntValues :  valuesMap.values()) {
      underlyingFlushes.add(activityIntValues.prepareFlush());
    }
    final String version = lastVersion;
    final int count;
    globalLock.readLock().lock();
    try {
    synchronized (deletedIndexes) {
      
      count = uidToArrayIndex.size() + deletedIndexes.size();
      currentDocumentsCounter.clear();
      currentDocumentsCounter.inc(uidToArrayIndex.size());
      reclaimedDocumentsCounter.clear();
      reclaimedDocumentsCounter.inc( deletedIndexes.size());
      logger.info("Flush compositeActivityValues. Documents = " +  uidToArrayIndex.size() + ", Deletes = " + deletedIndexes.size());
    }
    } finally {
      globalLock.readLock().unlock();
    }
     executor.submit(new Runnable() {      
      @Override
      public void run() {
        if (closed || activityStorage == null) {
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
    if (activityStorage != null) {
      activityStorage.close();
    }
    for (ActivityValues activityIntValues :  valuesMap.values()) {
      activityIntValues.close();
    }
  }
  
  public int[] precomputeArrayIndexes(long[] uids) {    
    int[] ret = new int[uids.length];   
    for (int i = 0; i < uids.length; i++) {
      long uid = uids[i];
      if (uid == ZoieIndexReader.DELETED_UID) {
        ret[i] = -1;
        continue;
      }
      Lock lock = globalLock.readLock();
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
    return valuesMap;
  }
  public int getIntValueByUID(long uid, String column) {
    Lock lock = globalLock.readLock();
    try {
    lock.lock();
    if (!uidToArrayIndex.containsKey(uid)) {
     return Integer.MIN_VALUE;
    }
    return ((ActivityIntValues)getActivityValues(column)).getIntValue(uidToArrayIndex.get(uid));
    } finally {
      lock.unlock();
    }
  }
  public float getFloatValueByUID(long uid, String column) {
    Lock lock = globalLock.readLock();
    try {
    lock.lock();
    if (!uidToArrayIndex.containsKey(uid)) {
     return Integer.MIN_VALUE;
    }
    return ((ActivityFloatValues)getActivityValues(column)).getFloatValue(uidToArrayIndex.get(uid));
    } finally {
      lock.unlock();
    }
  }
  public int getIndexByUID(long uid) {
    Lock lock = globalLock.readLock();
    try {
    lock.lock();
    if (!uidToArrayIndex.containsKey(uid)) {
     return -1;
    }
    return uidToArrayIndex.get(uid);
    } finally {
      lock.unlock();
    }
  }
  public  static CompositeActivityValues createCompositeValues(ActivityPersistenceFactory activityPersistenceFactory, Collection<SenseiSchema.FieldDefinition> fieldNames, List<TimeAggregateInfo> aggregatedActivities, Comparator<String> versionComparator) {    
    CompositeActivityValues ret = new CompositeActivityValues();
    CompositeActivityStorage persistentColumnManager = activityPersistenceFactory.getCompositeStorage();
   
    ret.metadata = activityPersistenceFactory.getMetadata();
    ret.activityConfig = activityPersistenceFactory.getActivityConfig();
    ret.updateBatch = new UpdateBatch<Update>(ret.activityConfig); 
    ret.pendingDeletes =  new UpdateBatch<Update>(ret.activityConfig); 
    ret.recentlyAddedUids = new RecentlyAddedUids(ret.activityConfig.getUndeletableBufferSize());
    int count = 0;
    if (ret.metadata != null) {
      ret.metadata.init();
      ret.lastVersion = ret.metadata.version;
      count = ret.metadata.count;
    }
    if (persistentColumnManager != null) {   
      persistentColumnManager.decorateCompositeActivityValues(ret, ret.metadata);
      //metadata might be trimmed
      count = ret.metadata.count;
    }
        
    logger.info("Init compositeActivityValues. Documents = " +  ret.uidToArrayIndex.size() + ", Deletes = " +ret.deletedIndexes.size());    
    ret.versionComparator = versionComparator;
   
   
    ret.valuesMap = new HashMap<String, ActivityValues>(fieldNames.size());
    for (TimeAggregateInfo aggregatedActivity : aggregatedActivities) {
      ret.valuesMap.put(aggregatedActivity.fieldName,  TimeAggregatedActivityValues.createTimeAggregatedValues(aggregatedActivity.fieldName, aggregatedActivity.times, count,
          activityPersistenceFactory));
    }
    for (SenseiSchema.FieldDefinition field : fieldNames) {
      if (field.isActivity && !ret.valuesMap.containsKey(field.name)) {
        ActivityPrimitiveValues values = ActivityPrimitiveValues.createActivityPrimitiveValues(activityPersistenceFactory, field, count);
        ret.valuesMap.put(field.name, values);
      }
    }    
    return ret;
  }
 
}
