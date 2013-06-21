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
package com.senseidb.indexing.activity.primitives;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;

import com.senseidb.indexing.activity.AtomicFieldUpdate;

public class ActivityFloatValues extends ActivityPrimitiveValues {
public float[] fieldValues;
  
  public void init(int capacity) {
    fieldValues = new float[capacity];
  }
  /* (non-Javadoc)
   * @see com.senseidb.indexing.activity.ActivityValues#update(int, java.lang.Object)
   */
  @Override
  public boolean update(int index, Object value) {
    ensureCapacity(index);
    if (fieldValues[index] == Float.MIN_VALUE) {
      fieldValues[index] = 0;
    }
    setValue(fieldValues, value, index);
    return updateBatch.addFieldUpdate(AtomicFieldUpdate.valueOf(index, fieldValues[index]));
  }

  protected ActivityFloatValues() {
    
  }
  public ActivityFloatValues(int capacity) {
    init(capacity);
  }

  public float getFloatValue(int index) {
    return fieldValues[index];
  }

  private synchronized void ensureCapacity(int currentArraySize) {
    if (fieldValues.length == 0) {
      this.fieldValues = new float[50000];
      return;
    }
    if (fieldValues.length - currentArraySize < 2) {
      int newSize = fieldValues.length < 10000000 ? fieldValues.length * 2 : (int) (fieldValues.length * 1.5);
      float[] newFieldValues = new float[newSize];
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
  private static void setValue(float[] fieldValues, Object value, int index) {
    if (value == null) {
      return;
    }
     if (value instanceof Number) {
      fieldValues[index] = ((Number) value).floatValue();
    } else if (value instanceof String) {
      String valStr = (String) value;
      if (valStr.isEmpty()) {
        return;
      }
      if (valStr.startsWith("+")) {
        fieldValues[index] = fieldValues[index] + Float.parseFloat(valStr.substring(1));
      } else if (valStr.startsWith("-")) {
        fieldValues[index] = fieldValues[index] + Float.parseFloat(valStr);
      } else {
        fieldValues[index] = Float.parseFloat(valStr);
      }
    } else {
      throw new UnsupportedOperationException(
          "Only longs, ints and String are supported");
    }
  }

  public float[] getFieldValues() {
    return fieldValues;
  }

  public void setFieldValues(float[] fieldValues) {
    this.fieldValues = fieldValues;
  }
  @Override 
  public void initFieldValues(int count, MappedByteBuffer  buffer) {
     for (int i = 0; i < count; i++) {
       float value;
         value = buffer.getFloat(i * 4);
       fieldValues[i] = value;
     }
   }
   @Override
   public void initFieldValues(int count, RandomAccessFile storedFile) {
     for (int i = 0; i < count; i++) {
       float value;       
         try {
          storedFile.seek(i * 4);
          value = storedFile.readFloat();       
          fieldValues[i] = value;
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
     }
   }
   @Override
   public void delete(int index) {
     fieldValues[index] = Float.MIN_VALUE;
     updateBatch.addFieldUpdate(AtomicFieldUpdate.valueOf(index, fieldValues[index]));   
   }
  @Override
  public int getFieldSizeInBytes() {
    
    return 4;
  }
  @Override
  public Number getValue(int index) {
    
    return getFloatValue(index);
  }

}
