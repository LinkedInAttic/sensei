package com.senseidb.indexing.activity;

import org.apache.log4j.Logger;

/**
 * Wraps an int array. Also provides the persistence support. The changes are kept accumulating in the batch.   
 *
 */
public class ActivityIntValues implements ActivityValues {
  private static Logger logger = Logger.getLogger(ActivityIntValues.class);
  public int[] fieldValues;
  protected String fieldName;
  protected ActivityIntStorage activityFieldStore;
  protected volatile UpdateBatch<ActivityIntStorage.FieldUpdate> updateBatch = new UpdateBatch<ActivityIntStorage.FieldUpdate>();
  

  @Override
  public void init(int capacity) {
    fieldValues = new int[capacity];
  }

  public void init() {
    init(50000);
  }

  
  /* (non-Javadoc)
   * @see com.senseidb.indexing.activity.ActivityValues#update(int, java.lang.Object)
   */
  @Override
  public boolean update(int index, Object value) {
    ensureCapacity(index);
    if (fieldValues[index] == Integer.MIN_VALUE) {
      fieldValues[index] = 0;
    }
    setValue(fieldValues, value, index);
    return updateBatch.addFieldUpdate(new ActivityIntStorage.FieldUpdate(index, fieldValues[index]));
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.senseidb.indexing.activity.ActivityValues#delete(int)
   */
  @Override
  public void delete(int index) {
    fieldValues[index] = Integer.MIN_VALUE;
  }
  protected ActivityIntValues() {
    
  }
  public ActivityIntValues(int capacity) {
    init(capacity);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.senseidb.indexing.activity.ActivityValues#prepareFlush()
   */
  @Override
  public Runnable prepareFlush() {
    if (activityFieldStore.isClosed()) {
      throw new IllegalStateException("The activityFile is closed");
    }
    final UpdateBatch<ActivityIntStorage.FieldUpdate> oldBatch = updateBatch;
    updateBatch = new UpdateBatch<ActivityIntStorage.FieldUpdate>();
    return new Runnable() {
      public void run() {
        try {
          if (activityFieldStore.isClosed()) {
            throw new IllegalStateException("The activityFile is closed");
          }
          activityFieldStore.flush(oldBatch.getUpdates());
        } catch (Exception ex) {
          logger.error("Failure to store the field values to file" + oldBatch.getUpdates(), ex);
        }
      }
    };
  }

  public int getValue(int index) {
    return fieldValues[index];
  }

  private synchronized void ensureCapacity(int currentArraySize) {
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

  /**
   * value might be int or long or String. +n, -n  operations are supported
   * @param fieldValues
   * @param value
   * @param index
   */
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
      throw new UnsupportedOperationException(
          "Only longs, ints and String are supported");
    }
  }

  public int[] getFieldValues() {
    return fieldValues;
  }

  public void setFieldValues(int[] fieldValues) {
    this.fieldValues = fieldValues;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.senseidb.indexing.activity.ActivityValues#close()
   */
  @Override
  public void close() {
    activityFieldStore.close();
  }

  /**
   * The factory method to create the ActivityIntValues. If the corresponding file doesn't exist, the new one will be created
   * @param indexDirPath
   * @param fieldName
   * @param the number of records in the file. This info is ussually kept separately in the metadata files
   * @return
   */
  public static ActivityIntValues readFromFile(String indexDirPath,
      String fieldName, int count) {
    ActivityIntStorage persistentColumnManager = new ActivityIntStorage(fieldName, indexDirPath);
    persistentColumnManager.init();
    return persistentColumnManager.getActivityDataFromFile(count);
  }

  @Override
  public String getFieldName() {
    return fieldName;
  }

}
