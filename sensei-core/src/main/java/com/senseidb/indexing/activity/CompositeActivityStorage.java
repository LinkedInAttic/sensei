package com.senseidb.indexing.activity;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.util.Comparator;
import java.util.List;

import org.apache.log4j.Logger;
import org.springframework.util.Assert;

import com.senseidb.indexing.activity.ActivityIntStorage.FieldUpdate;

public class CompositeActivityStorage {
  private static Logger logger = Logger.getLogger(ActivityIntStorage.class);
  private RandomAccessFile storedFile;  
  private final String indexDir;
  private volatile boolean closed = false;
  private MappedByteBuffer buffer;
  private long fileLength; 
  private boolean activateMemoryMappedBuffers = true;
  public CompositeActivityStorage(String indexDir) {   
    this.indexDir = indexDir;
  }

  public synchronized void init() {
    try {
      File file = new File(indexDir,  "activity.indexes");
      if (!file.exists()) {
        file.createNewFile();
      }
      storedFile = new RandomAccessFile(file, "rw");
      fileLength = storedFile.length();
       if (activateMemoryMappedBuffers){
         buffer = storedFile.getChannel().map(MapMode.READ_WRITE, 0, file.length());       
       }
    } catch (IOException e) {
      logger.error(e.getMessage(), e);
      throw new RuntimeException(e);
    }
  }

  public synchronized void flush(List<Update> updates) {
    Assert.state(storedFile != null, "The FileStorage is not initialized");    
    try {
      for (Update update : updates) {       
         ensureCapacity(update.index * 8 + 8);        
         if (activateMemoryMappedBuffers) {
           buffer.putLong(update.index * 8, update.value);
         } else {
           storedFile.seek(update.index * 8);
           storedFile.writeLong(update.value); 
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

  protected CompositeActivityValues getActivityDataFromFile(Metadata metadata) {
    Assert.state(storedFile != null, "The FileStorage is not initialized");
    CompositeActivityValues ret = new CompositeActivityValues();
    ret.activityStorage = this;   
    try {
      if (metadata.count == 0) {
        ret.init();
        return ret;
      }
      ret.init((int) (metadata.count * 1.5));
      for (int i = 0; i < metadata.count; i++) {
        long value;
        if (activateMemoryMappedBuffers) {
          value = buffer.getLong(i * 8);
        }
        else {
          storedFile.seek(i * 8);
          value = storedFile.readLong();
        }
        if (value != Long.MIN_VALUE) {
          ret.uidToArrayIndex.put(value, i);
        }
        else {
          ret.deletedIndexes.add(i);
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return ret;
  }
  
  public boolean isClosed() {
    return closed;
  }  

  public static class Update {
    public int index;   
    public long value;
    public Update(int index, long value) {
      this.index = index;
      this.value = value;
    }
    @Override
    public String toString() {
      return "index=" + index + ", value=" + value;
    }    
  } 

}
