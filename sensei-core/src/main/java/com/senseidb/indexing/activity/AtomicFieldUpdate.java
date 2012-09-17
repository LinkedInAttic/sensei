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

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;

public abstract class AtomicFieldUpdate {
  public int index;   
  public abstract int getFieldSizeInBytes();
  public abstract void update(MappedByteBuffer mappedByteBuffer, int offset);
  public abstract void update(RandomAccessFile storedFile, int offset);
  
  public static class IntFieldUpdate extends AtomicFieldUpdate {
    public int value;
    @Override
    public int getFieldSizeInBytes() {      
      return 4;
    }
    @Override
    public void update(MappedByteBuffer mappedByteBuffer, int offset) {
      mappedByteBuffer.putInt(offset, value);
    }

    @Override
    public void update(RandomAccessFile storedFile, int offset) {
      try {
        storedFile.seek(offset);
        storedFile.writeInt(value);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }      
    } 
  }
  public static AtomicFieldUpdate valueOf(int index, int value) {
    IntFieldUpdate ret = new IntFieldUpdate();
    ret.index = index;
    ret.value = value;
    return ret;
  }
  public static AtomicFieldUpdate valueOf(int index, float value) {
    FloatFieldUpdate ret = new FloatFieldUpdate();
    ret.index = index;
    ret.value = value;
    return ret;
  }
  public static class FloatFieldUpdate extends AtomicFieldUpdate {
    public float value;
    @Override
    public int getFieldSizeInBytes() {      
      return 4;
    }
    @Override
    public void update(MappedByteBuffer mappedByteBuffer, int offset) {
      mappedByteBuffer.putFloat(offset, value);
    }

    @Override
    public void update(RandomAccessFile storedFile, int offset) {
      try {
        storedFile.seek(offset);
        storedFile.writeFloat(value);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }      
    } 
  }
  
  
}
