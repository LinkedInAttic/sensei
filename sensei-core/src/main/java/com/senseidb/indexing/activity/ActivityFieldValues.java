package com.senseidb.indexing.activity;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.longs.Long2IntMap;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongList;

import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.log4j.Logger;

import proj.zoie.api.ZoieIndexReader;

import com.senseidb.indexing.activity.FileStorage.FieldUpdate;
import com.senseidb.indexing.activity.FileStorage.UpdateBatch;

public class ActivityFieldValues {
  private static Logger logger = Logger.getLogger(ActivityFieldValues.class);
  protected int[] fieldValues;
  protected String fieldName;
  protected volatile String lastVersion = "";
  protected volatile String lastPersistedVersion;
  protected Long2IntMap uidToArrayIndex = new Long2IntOpenHashMap();  
  private ReadWriteLock[] locks;
  protected Comparator<String> versionComparator;
  protected int arraySize;
  private UpdateBatch updateBatch;
  private ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
  protected FileStorage activityFieldStore;
  protected IntList deletedIndexes = new IntArrayList(2000);
  
  public void init(int capacity) {
    uidToArrayIndex = new Long2IntOpenHashMap(capacity);
    locks = new ReadWriteLock[1024];
    for (int i = 0; i < 1024; i++) {
      locks[i] = new ReentrantReadWriteLock();
    }
    fieldValues = new int[capacity];
    updateBatch = new FileStorage.UpdateBatch();
  }

  public void init() {
    init(50000);
  }
  
  public void update(long uid, final String version, Object value) {
    if (versionComparator.compare(lastVersion, version) > 0) {
      return;
    }
    int index = -1;
    ReadWriteLock lock = getLock(uid);
    Lock writeLock = lock.writeLock();
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
             fieldValues[index] = 0;
             deletedDocsPresent = true;
          }
        }
        if (!deletedDocsPresent) {
          ensureCapacity(fieldValues, arraySize);
          index = arraySize;
          uidToArrayIndex.put(uid, index);          
          arraySize++;
        }
      }
      setValue(fieldValues, value, index);
    } finally {
      writeLock.unlock();
    }
    boolean needToFlush = updateBatch.addFieldUpdate(new FileStorage.FieldUpdate(index, uid, fieldValues[index], version));
    if (needToFlush) {
      flush();
    }
    lastVersion = version;
  }
 
  public void delete(LongList uids) {   
    final IntList indexes = new IntArrayList(uids.size());
    for (long uid : uids) {
    ReadWriteLock lock = getLock(uid);
    Lock writeLock = lock.writeLock();
    int index = -1;
    try {
      writeLock.lock();
      if (!uidToArrayIndex.containsKey(uid)) {
        if (logger.isDebugEnabled()) {
          logger.debug("Couldn't delete the document, as it doesn't exist. UID = " + uid);
        }
        continue;
      }
      index =  uidToArrayIndex.remove(uid);
      fieldValues[index] = Integer.MIN_VALUE;
      indexes.add(index);
    } finally {
      writeLock.unlock();
    }
    }
    executor.execute(new Runnable() {
      public void run() {       
          if (activityFieldStore.isClosed()) {
            return;
          }
          activityFieldStore.delete(indexes);  
          activityFieldStore.commit(lastPersistedVersion, arraySize);
          synchronized (deletedIndexes) {
            deletedIndexes.addAll(indexes);
          }
      }});
  }
  
  
  public void flush() {
    if (updateBatch.getUpdates().isEmpty()) {      
      return;
    }
    if (activityFieldStore.isClosed()) {
      return;
    }
    final FileStorage.UpdateBatch oldBatch = updateBatch;
    updateBatch = new FileStorage.UpdateBatch();
    final String newVersion = getMaxVersion(oldBatch.getUpdates());
    final int capacity = arraySize;
    executor.execute(new Runnable() {
      public void run() {
        try {
          if (activityFieldStore.isClosed()) {
            return;
          }
          if (versionComparator.compare(newVersion, lastPersistedVersion) < 0) {
            throw new IllegalArgumentException("The current version couln't be less than the one on persisted activity values");
          }
          activityFieldStore.applyUpdates(oldBatch.getUpdates());         
          activityFieldStore.commit(newVersion, capacity);
          lastPersistedVersion = newVersion;
        } catch (Exception ex) {
          logger.error("Failure to store the field values to file" + oldBatch.getUpdates(), ex);
        }
      }
    });
  }

  private String getMaxVersion(List<FieldUpdate> updates) {
    String ret = updates.get(0).version;
    for (int i = 1; i < updates.size(); i ++) {
      if (versionComparator.compare(ret, updates.get(i).version) < 0) {
        ret = updates.get(i).version;
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

  public int getValue(int index) {
    return fieldValues[index];
  }
  
  public int getValueByUID(long uid) {
    Lock readLock = getLock(uid).readLock();
    if (!uidToArrayIndex.containsKey(uid)) {
      return Integer.MIN_VALUE;
    }
    try {
      readLock.lock();
      return fieldValues[uidToArrayIndex.get(uid)];
    } finally {
      readLock.unlock();
    }
  }
  private void ensureCapacity(int[] fieldValues, int currentArraySize) {
    if (fieldValues.length == 0) {
      this.fieldValues = new int[50000];
      return;
    }
    if (fieldValues.length - currentArraySize < 2) {
      int newSize = fieldValues.length < 10000000 ? fieldValues.length * 2 : (int) (fieldValues.length * 1.5);
      int[] newFieldValues = new int[newSize];
      System.arraycopy(fieldValues, 0, newFieldValues, 0, fieldValues.length);
      this.fieldValues = newFieldValues;
    }
  }
  public static void setValue(int[] fieldValues, Object value, int index) {
    if (value == null) {
      return;
    }
    if (value instanceof Integer) {
      fieldValues[index] = (Integer) value;
    } else if (value instanceof Long) {
      fieldValues[index] = ((Long) value).intValue();
    } else if (value instanceof String) {
      String valStr = (String) value;
      if (valStr.isEmpty()) {
        return;
      }
      if (valStr.startsWith("+")) {
        fieldValues[index] = fieldValues[index] + Integer.parseInt(valStr.substring(1));
      } else if (valStr.startsWith("-")) {
        fieldValues[index] = fieldValues[index] + Integer.parseInt(valStr);
      } else {
        fieldValues[index] = Integer.parseInt(valStr);
      }
    } else {
      throw new UnsupportedOperationException("Only longs, ints and String are supported");
    }    
  }

  protected ReadWriteLock getLock(long uid) {
    return locks[(int) (uid % locks.length)];
  }

  public String getVersion() {
    return lastVersion;
  }

  public void syncWithPersistentVersion(String version) {
    synchronized (this) {
      while (versionComparator.compare(lastPersistedVersion, version) < 0) {
        try {
          this.wait(400L);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    }
  }

  public void syncWithVersion(long timeToWait, String version) {
    synchronized (this) {
      long startTime = System.currentTimeMillis();
      while (versionComparator.compare(lastVersion, version) < 0) {
        try {
          this.wait(50L);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        if (startTime + timeToWait < System.currentTimeMillis()) {
          throw new IllegalStateException("sync timed out at current: " + lastVersion + " expecting: " + version);
        }
      }
    }
  }
  
  public int[] getFieldValues() {
    return fieldValues;
  }

  public void setFieldValues(int[] fieldValues) {
    this.fieldValues = fieldValues;
  }

  public void close() {
    activityFieldStore.close();
  }

  public static ActivityFieldValues readFromFile(String indexDirPath, String fieldName, Comparator<String> versionComparator) {    
    FileStorage persistentColumnManager = new FileStorage(fieldName, indexDirPath);
    persistentColumnManager.init();
    return persistentColumnManager.getActivityDataFromFile(versionComparator);
  }

  
}
