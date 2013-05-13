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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
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
import com.senseidb.indexing.activity.primitives.ActivityLongValues;
import com.senseidb.indexing.activity.primitives.ActivityPrimitiveValues;
import com.senseidb.indexing.activity.time.TimeAggregatedActivityValues;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.MetricName;

/**
 * 
 * Maintains the set of activityValues. The main responsibility of this class is
 * to keep track of uid to array index mapping, persisted and in memory
 * versions. The the document gets into the system, the class will find/create
 * uid to index mapping, and change the activity values for the activity fields
 * found in the document
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
  protected AtomicInteger indexSize;
  protected volatile Metadata metadata;

  private volatile boolean closed;
  private ActivityConfig activityConfig;

  protected static Counter reclaimedDocumentsCounter;
  protected static Counter currentDocumentsCounter;
  protected static Counter deletedDocumentsCounter;
  protected static Counter insertedDocumentsCounter;
  protected static Counter totalUpdatesCounter;
  protected static Counter versionRejectionCounter;
  static {
    reclaimedDocumentsCounter = Metrics.newCounter(new MetricName(CompositeActivityValues.class, "reclaimedActivityDocs"));
    currentDocumentsCounter = Metrics.newCounter(new MetricName(CompositeActivityValues.class, "currentActivityDocs"));
    deletedDocumentsCounter = Metrics.newCounter(new MetricName(CompositeActivityValues.class, "deletedActivityDocs"));
    insertedDocumentsCounter = Metrics.newCounter(new MetricName(CompositeActivityValues.class, "insertedActivityDocs"));
    totalUpdatesCounter = Metrics.newCounter(new MetricName(CompositeActivityValues.class, "totalUpdatesCounter"));
    versionRejectionCounter = Metrics.newCounter(new MetricName(CompositeActivityValues.class, "activityVersionRejectionCounter"));
  }

  CompositeActivityValues() {

  }

  public void init() {
    init(DEFAULT_INITIAL_CAPACITY);
  }

  public void init(int count) {
    uidToArrayIndex = new Long2IntOpenHashMap(count);
    indexSize = new AtomicInteger(0);
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
      versionRejectionCounter.inc();
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
            index = indexSize.getAndIncrement();
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
        return ((TimeAggregatedActivityValues) valuesMap.get(fieldName.substring(0, fieldName.indexOf(":")))).getValuesMap().get(
            fieldName.substring(fieldName.indexOf(":") + 1));
      }
      return null;
    } else if (activityValues instanceof ActivityIntValues) {
      return (ActivityIntValues) activityValues;
    } else if (activityValues instanceof ActivityFloatValues) {
      return (ActivityFloatValues) activityValues;
    } else if (activityValues instanceof ActivityLongValues) {
        return (ActivityLongValues) activityValues;
      } else {
      return ((TimeAggregatedActivityValues) activityValues).getDefaultIntValues();
    }
  }

  private boolean updateActivities(Map<String, Object> map, int index) {
    boolean needToFlush = false;
    for (ActivityValues activityIntValues : valuesMap.values()) {
      Object value = map.get(activityIntValues.getFieldName());
      if (value != null) {
        needToFlush = needToFlush | activityIntValues.update(index, value);
      } else {
        needToFlush = needToFlush | activityIntValues.update(index, "+0");
      }
    }
    return needToFlush;
  }

  /**
   * Deletes documents from the activity engine
   * 
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
        for (ActivityValues activityIntValues : valuesMap.values()) {
          activityIntValues.delete(index);
        }
        needToFlush = needToFlush | pendingDeletes.addFieldUpdate(new Update(index, Long.MIN_VALUE));
      } finally {
        writeLock.unlock();
      }
    }
    if (needToFlush) {
      flush();
    }
  }

  /**
   * Propagates the deletes to disk. After calling this method freed array
   * indexes can be reused for different document uids
   */
  /*private void flushDeletes() {
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
  }*/

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

    if (closed || activityStorage == null) {
      return;
    }
    final boolean flushDeletesNeeded = pendingDeletes.updates.size() > 0;
    final UpdateBatch<Update> batchToDelete = (flushDeletesNeeded) ? pendingDeletes : null;
    if (flushDeletesNeeded) {
      pendingDeletes = new UpdateBatch<Update>(activityConfig);
    }
    final boolean flushUpdatesNeeded = updateBatch.updates.size() > 0 || versionComparator.compare(lastVersion, metadata.version) != 0;
    if (!flushUpdatesNeeded && !flushDeletesNeeded) {
      return;
    }
    final UpdateBatch<Update> batchToPersist = flushUpdatesNeeded ? updateBatch : null;
    final List<Runnable> underlyingFlushes = new ArrayList<Runnable>(valuesMap.size());
    //IF WE DON'T NEED TO FLUSH UPDATES,let's keep the persistent version as it is
    final String version = flushUpdatesNeeded ? lastVersion : metadata.version;
    if (flushUpdatesNeeded) {
      updateBatch = new UpdateBatch<CompositeActivityStorage.Update>(activityConfig);
    }
    for (ActivityValues activityIntValues : valuesMap.values()) {
      underlyingFlushes.add(activityIntValues.prepareFlush());
    }
    executor.submit(new Runnable() {
      @Override
      public void run() {
        if (closed) {
          return;
        }
        if (flushUpdatesNeeded) {
          activityStorage.flush(batchToPersist.updates);
        }
        if (flushDeletesNeeded) {
          Collections.reverse(batchToDelete.updates);
          activityStorage.flush(batchToDelete.updates);
          synchronized (deletedIndexes) {
            for (Update update : batchToDelete.updates) {
              deletedIndexes.add(update.index);
            }
          }
        }
        int count = 0;
        globalLock.readLock().lock();
        try {
          synchronized (deletedIndexes) {
            count = uidToArrayIndex.size() + deletedIndexes.size();
            currentDocumentsCounter.clear();
            currentDocumentsCounter.inc(uidToArrayIndex.size());
            reclaimedDocumentsCounter.clear();
            reclaimedDocumentsCounter.inc(deletedIndexes.size());
            logger.info("Flush compositeActivityValues. Documents = " + uidToArrayIndex.size() + ", Deletes = " + deletedIndexes.size());
          }
        } finally {
          globalLock.readLock().unlock();
        }
        for (Runnable runnable : underlyingFlushes) {
          runnable.run();
        }
        metadata.update(version, count);
      }
    });

  }

  public void close() {
    closed = true;
    executor.shutdown();
    try {
      executor.awaitTermination(2, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    if (activityStorage != null) {
      activityStorage.close();
    }
    for (ActivityValues activityIntValues : valuesMap.values()) {
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
      return ((ActivityIntValues) getActivityValues(column)).getIntValue(uidToArrayIndex.get(uid));
    } finally {
      lock.unlock();
    }
  }

  public float getFloatValueByUID(long uid, String column) {
    Lock lock = globalLock.readLock();
    try {
      lock.lock();
      if (!uidToArrayIndex.containsKey(uid)) {
        return Float.MIN_VALUE;
      }
      return ((ActivityFloatValues) getActivityValues(column)).getFloatValue(uidToArrayIndex.get(uid));
    } finally {
      lock.unlock();
    }
  }
  public long getLongValueByUID(long uid, String column) {
      Lock lock = globalLock.readLock();
      try {
        lock.lock();
        if (!uidToArrayIndex.containsKey(uid)) {
          return Long.MIN_VALUE;
        }
        return ((ActivityLongValues) getActivityValues(column)).getLongValue(uidToArrayIndex.get(uid));
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

  public static CompositeActivityValues createCompositeValues(ActivityPersistenceFactory activityPersistenceFactory,
      Collection<SenseiSchema.FieldDefinition> fieldNames, List<TimeAggregateInfo> aggregatedActivities,
      Comparator<String> versionComparator) {
    CompositeActivityValues ret = new CompositeActivityValues();
    CompositeActivityStorage persistentColumnManager = activityPersistenceFactory.getCompositeStorage();

    ret.metadata = activityPersistenceFactory.getMetadata();

    ret.activityConfig = activityPersistenceFactory.getActivityConfig();
    ret.updateBatch = new UpdateBatch<Update>(ret.activityConfig);
    ret.pendingDeletes = new UpdateBatch<Update>(ret.activityConfig);
    ret.recentlyAddedUids = new RecentlyAddedUids(ret.activityConfig.getUndeletableBufferSize());

    int count = 0;
    if (ret.metadata != null) {
      ret.metadata.init();
      ret.lastVersion = ret.metadata.version;
      count = ret.metadata.count;
    }
    if (persistentColumnManager != null) {
      persistentColumnManager.decorateCompositeActivityValues(ret, ret.metadata);
      // metadata might be trimmed
      count = ret.metadata.count;
    }

    logger.info("Init compositeActivityValues. Documents = " + ret.uidToArrayIndex.size() + ", Deletes = " + ret.deletedIndexes.size());
    ret.versionComparator = versionComparator;

    ret.valuesMap = new HashMap<String, ActivityValues>(fieldNames.size());
    for (TimeAggregateInfo aggregatedActivity : aggregatedActivities) {
      ret.valuesMap.put(aggregatedActivity.fieldName, TimeAggregatedActivityValues.createTimeAggregatedValues(aggregatedActivity.fieldName,
          aggregatedActivity.times, count, activityPersistenceFactory));
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
