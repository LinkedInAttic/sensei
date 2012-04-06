package com.senseidb.indexing.activity;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.util.List;

import org.apache.log4j.Logger;
import org.springframework.util.Assert;

public class ActivityIntStorage {
  private static Logger logger = Logger.getLogger(ActivityIntStorage.class);
  private RandomAccessFile storedFile;
  private final String fieldName;
  private final String indexDir;
  private volatile boolean closed = false;
  private MappedByteBuffer buffer;
  private long fileLength; 
  private boolean activateMemoryMappedBuffers = true;
  public ActivityIntStorage(String fieldName, String indexDir) {
    this.fieldName = fieldName;
    this.indexDir = indexDir;
  }

  public synchronized void init() {
    try {
      String fileName = fieldName.replace(':', '-');
      File file = new File(indexDir, fileName + ".data");
      if (!file.exists()) {
        file.createNewFile();
      }
      storedFile = new RandomAccessFile(file, "rw");
      fileLength = storedFile.length();
      if (activateMemoryMappedBuffers) {
        buffer = storedFile.getChannel().map(MapMode.READ_WRITE, 0, file.length());     
      } 
    } catch (IOException e) {
      logger.error(e.getMessage(), e);
      throw new RuntimeException(e);
    }
  }

  public synchronized void flush(List<FieldUpdate> updates) {
    Assert.state(storedFile != null, "The FileStorage is not initialized");    
    try {
      for (FieldUpdate update : updates) {       
         ensureCapacity(update.index * 4 + 4);        
         if (activateMemoryMappedBuffers) {
         buffer.putInt(update.index * 4, update.value);
         } else {  
           storedFile.seek(update.index * 4);
           storedFile.writeInt(update.value);
         }
      }
      if (activateMemoryMappedBuffers) {
        buffer.force();
      } else {
        storedFile.getFD().sync(); 
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
  private void ensureCapacity( int i) {
    try {
    if (fileLength > i + 100) {
      return;
    }
   
    if (fileLength > 1000000) {
      fileLength = fileLength * 2;
    } else {
      fileLength = 2000000;
    }
    storedFile.setLength(fileLength);
    if (activateMemoryMappedBuffers) {
      buffer = storedFile.getChannel().map(MapMode.READ_WRITE, 0,  fileLength);
    }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
  public synchronized void close() {
    try {
      if (activateMemoryMappedBuffers) {
        buffer.force();
      }
      storedFile.close();
      closed = true;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  protected  ActivityIntValues getActivityDataFromFile(int count) {
    Assert.state(storedFile != null, "The FileStorage is not initialized");
    ActivityIntValues ret = new ActivityIntValues();
    ret.activityFieldStore = this;
    ret.fieldName = fieldName;
    try {
      if (count == 0) {
        ret.init();
        return ret;
      }
      ret.init((int) (count * 1.5));
      for (int i = 0; i < count; i++) {
        int value;
        if (activateMemoryMappedBuffers) {
          value = buffer.getInt(i * 4);
        } else {
          storedFile.seek(i * 4);
          value = storedFile.readInt();
        }
        ret.fieldValues[i] = value;
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return ret;
  }
  
  public boolean isClosed() {
    return closed;
  }  

  public static class FieldUpdate {
    public int index;   
    public int value;
    public FieldUpdate(int index, int value) {
      this.index = index;
      this.value = value;
    }
    @Override
    public String toString() {
      return "index=" + index + ", value=" + value;
    }    
  }
}
