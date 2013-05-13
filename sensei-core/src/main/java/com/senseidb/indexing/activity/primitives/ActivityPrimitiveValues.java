package com.senseidb.indexing.activity.primitives;

import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;

import org.apache.log4j.Logger;

import com.senseidb.conf.SenseiSchema;

import com.senseidb.indexing.activity.ActivityConfig;

import com.senseidb.indexing.activity.ActivityPersistenceFactory;
import com.senseidb.indexing.activity.ActivityValues;
import com.senseidb.indexing.activity.AtomicFieldUpdate;
import com.senseidb.indexing.activity.UpdateBatch;

public abstract class ActivityPrimitiveValues implements ActivityValues {

  private static Logger logger = Logger.getLogger(ActivityIntValues.class);
  protected String fieldName;
  protected ActivityPrimitivesStorage activityFieldStore;

  protected volatile UpdateBatch<AtomicFieldUpdate> updateBatch;
  private ActivityConfig activityConfig;


  public ActivityPrimitiveValues() {
    super();
  }

  public void init() {
    init(50000);
  }

  public abstract void init(int count);

  @Override
  public Runnable prepareFlush() {
    if (activityFieldStore == null) {
      return new Runnable() {        
        public void run() {
        }
      };
    }
    if (activityFieldStore.isClosed()) {
      throw new IllegalStateException("The activityFile is closed");
    }
    final UpdateBatch<AtomicFieldUpdate> oldBatch = updateBatch;

    updateBatch = new UpdateBatch<AtomicFieldUpdate>(activityConfig);

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

  @Override
  public void close() {
    if (activityFieldStore != null) {
      activityFieldStore.close();
    }
  }

  @Override
  public String getFieldName() {
    return fieldName;
  }

  public abstract void initFieldValues(int count, RandomAccessFile storedFile);

  public abstract void initFieldValues(int count, MappedByteBuffer buffer);

  public abstract int getFieldSizeInBytes();
  public abstract Number getValue(int index);
  
  public static ActivityPrimitiveValues createActivityPrimitiveValues(ActivityPersistenceFactory activityPersistenceFactory,
      SenseiSchema.FieldDefinition field, int count) {
    return createActivityPrimitiveValues(activityPersistenceFactory, field.type, field.name, count);
  }

  public static ActivityPrimitiveValues createActivityPrimitiveValues(ActivityPersistenceFactory activityPersistenceFactory, Class<?> type,
      String fieldName, int count) {
    ActivityPrimitiveValues values = null;
    if (type == int.class) {
      values = new ActivityIntValues();
    } else if (type == float.class || type == double.class) {
      values = new ActivityFloatValues();
    } else if (type == long.class) {
        values = new ActivityLongValues();
      } else
      throw new UnsupportedOperationException("Class " + type + " is not supported");
    ActivityPrimitivesStorage primitivesStorage = activityPersistenceFactory.getActivivityPrimitivesStorage(fieldName);
    values.fieldName = fieldName;

    values.activityConfig = activityPersistenceFactory.getActivityConfig();
    values.updateBatch = new UpdateBatch<AtomicFieldUpdate>(activityPersistenceFactory.getActivityConfig());

    if (primitivesStorage != null) {
      primitivesStorage.initActivityDataFromFile(values, count);
    } else {
      values.init();
    }
    return values;
  }
  
}