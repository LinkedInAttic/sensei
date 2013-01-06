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
  public static class LongFieldUpdate extends AtomicFieldUpdate {
      public long value;
      @Override
      public int getFieldSizeInBytes() {      
        return 8;
      }
      @Override
      public void update(MappedByteBuffer mappedByteBuffer, int offset) {
        mappedByteBuffer.putLong(offset, value);
      }

      @Override
      public void update(RandomAccessFile storedFile, int offset) {
        try {
          storedFile.seek(offset);
          storedFile.writeLong(value);
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
  public static AtomicFieldUpdate valueOf(int index, long value) {
      LongFieldUpdate ret = new LongFieldUpdate();
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
