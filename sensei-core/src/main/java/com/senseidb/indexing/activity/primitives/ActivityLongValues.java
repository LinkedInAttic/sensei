package com.senseidb.indexing.activity.primitives;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;

import com.senseidb.indexing.activity.AtomicFieldUpdate;

public class ActivityLongValues extends ActivityPrimitiveValues {
    public long[] fieldValues;
    
    public void init(int capacity) {
      fieldValues = new long[capacity];
    }
    /* (non-Javadoc)
     * @see com.senseidb.indexing.activity.ActivityValues#update(int, java.lang.Object)
     */
    @Override
    public boolean update(int index, Object value) {
      ensureCapacity(index);
      if (fieldValues[index] == Long.MIN_VALUE) {
        fieldValues[index] = 0;
      }
      setValue(fieldValues, value, index);
      return updateBatch.addFieldUpdate(AtomicFieldUpdate.valueOf(index, fieldValues[index]));
    }

    protected ActivityLongValues() {
      
    }
    public ActivityLongValues(int capacity) {
      init(capacity);
    }

    public long getLongValue(int index) {
      return fieldValues[index];
    }

    private synchronized void ensureCapacity(int currentArraySize) {
      if (fieldValues.length == 0) {
        this.fieldValues = new long[50000];
        return;
      }
      if (fieldValues.length - currentArraySize < 2) {
        int newSize = fieldValues.length < 10000000 ? fieldValues.length * 2 : (int) (fieldValues.length * 1.5);
        long[] newFieldValues = new long[newSize];
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
    private static void setValue(long[] fieldValues, Object value, int index) {
      if (value == null) {
        return;
      }
       if (value instanceof Number) {
        fieldValues[index] = ((Number) value).longValue();
      } else if (value instanceof String) {
        String valStr = (String) value;
        if (valStr.isEmpty()) {
          return;
        }
        if (valStr.startsWith("+")) {
          fieldValues[index] = fieldValues[index] + Long.parseLong(valStr.substring(1));
        } else if (valStr.startsWith("-")) {
          fieldValues[index] = fieldValues[index] + Long.parseLong(valStr);
        } else {
          fieldValues[index] = Long.parseLong(valStr);
        }
      } else {
        throw new UnsupportedOperationException(
            "Only longs, ints and String are supported");
      }
    }

    public long[] getFieldValues() {
      return fieldValues;
    }

    public void setFieldValues(long[] fieldValues) {
      this.fieldValues = fieldValues;
    }
    @Override 
    public void initFieldValues(int count, MappedByteBuffer  buffer) {
       for (int i = 0; i < count; i++) {
         long value;
           value = buffer.getLong(i * 8);
         fieldValues[i] = value;
       }
     }
     @Override
     public void initFieldValues(int count, RandomAccessFile storedFile) {
       for (int i = 0; i < count; i++) {
         long value;       
           try {
            storedFile.seek(i * 8);
            value = storedFile.readLong();       
            fieldValues[i] = value;
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
       }
     }
     @Override
     public void delete(int index) {
       fieldValues[index] = Long.MIN_VALUE;
       updateBatch.addFieldUpdate(AtomicFieldUpdate.valueOf(index, fieldValues[index]));   
     }
    @Override
    public int getFieldSizeInBytes() {
      return 8;
    }
    @Override
    public Number getValue(int index) {
      
      return getLongValue(index);
    }

}
