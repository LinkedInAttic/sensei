package com.senseidb.indexing.activity;

import org.apache.log4j.Logger;

public class ActivityIntValues {
  private static Logger logger = Logger.getLogger(ActivityIntValues.class);
  public int[] fieldValues;
  protected String fieldName; 
  protected ActivityIntStorage activityFieldStore;
  protected UpdateBatch<ActivityIntStorage.FieldUpdate> updateBatch = new UpdateBatch<ActivityIntStorage.FieldUpdate>(); 
  
  public void init(int capacity) {    
    fieldValues = new int[capacity];  
  }

  public void init() {
    init(50000);
  }
  
  public boolean update(int index, Object value) {
      ensureCapacity(fieldValues, index);
      if (fieldValues[index] == Integer.MIN_VALUE) {
        fieldValues[index] = 0;
      }
      setValue(fieldValues, value, index);
      return updateBatch.addFieldUpdate(new ActivityIntStorage.FieldUpdate(index, fieldValues[index])); 
  }
  
  public void delete(int index) {
    fieldValues[index] = Integer.MIN_VALUE;
  }
  
  protected ActivityIntValues() {
    
  }
  
  
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
  
  public int[] getFieldValues() {
    return fieldValues;
  }

  public void setFieldValues(int[] fieldValues) {
    this.fieldValues = fieldValues;
  }

  public void close() {
    activityFieldStore.close();
  }

  public static ActivityIntValues readFromFile(String indexDirPath, String fieldName, int count) {    
    ActivityIntStorage persistentColumnManager = new ActivityIntStorage(fieldName, indexDirPath);
    persistentColumnManager.init();
    return persistentColumnManager.getActivityDataFromFile(count);
  }

 
}
