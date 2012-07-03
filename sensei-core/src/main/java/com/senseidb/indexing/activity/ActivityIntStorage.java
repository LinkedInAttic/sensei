package com.senseidb.indexing.activity;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.springframework.util.Assert;

import com.senseidb.metrics.MetricsConstants;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.Timer;

/**
 * Allows to persist ActivityIntValues into the file. The persistence is asynchronous via {@link ActivityIntValues#prepareFlush()}
 *
 */
public class ActivityIntStorage {
  public static final double INIT_GROWTH_RATIO = 1.5;
  public static final int BYTES_IN_INT = 4;
  public static final int LENGTH_THRESHOLD = 1000000;
  public static final int FILE_GROWTH_RATIO = 2;
  public static final int INITIAL_FILE_LENGTH = 2000000;
  private static Logger logger = Logger.getLogger(ActivityIntStorage.class);
  private RandomAccessFile storedFile;
  private final String fieldName;
  private final String indexDir;
  private volatile boolean closed = false;
  private MappedByteBuffer buffer;
  private long fileLength; 
  private boolean activateMemoryMappedBuffers = true;
  private Timer timer;
  private String fileName;
  
  public ActivityIntStorage(String fieldName, String indexDir) {
    this.fieldName = fieldName;
    this.indexDir = indexDir;
    timer = Metrics.newTimer(new MetricName(MetricsConstants.Domain,"timer","initIntActivities-time-" + fieldName.replaceAll(":", "-"),"initIntActivities"), TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
   
  }

  public synchronized void init() {
    try {
      fileName = fieldName.replace(':', '-');
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
      }
      storedFile.getFD().sync(); 
     
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
  private void ensureCapacity( int i) {
    try {
    if (fileLength > i + 100) {
      return;
    }
   
    if (fileLength > LENGTH_THRESHOLD) {
      fileLength = fileLength * FILE_GROWTH_RATIO;
    } else {
      fileLength = INITIAL_FILE_LENGTH;
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

  protected  ActivityIntValues getActivityDataFromFile(final int count) {
    try {
      return timer.time(new Callable<ActivityIntValues>() {
        @Override
        public ActivityIntValues call() throws Exception {
          Assert.state(storedFile != null, "The FileStorage is not initialized");
          ActivityIntValues ret = new ActivityIntValues();
          ret.activityFieldStore = ActivityIntStorage.this;
          ret.fieldName = fieldName;
          try {
            if (count == 0) {
              ret.init();
              return ret;
            }
            ret.init((int) (count * INIT_GROWTH_RATIO));
            if (fileLength < count * BYTES_IN_INT) {
              logger.warn("The  activityIndex is corrupted. The file "+ fieldName +" contains " + (fileLength / BYTES_IN_INT) + " records, while metadata has a bigger number " + count);
              logger.warn("adding extra space");
              ensureCapacity(count * BYTES_IN_INT);
            }
            for (int i = 0; i < count; i++) {
              int value;
              if (activateMemoryMappedBuffers) {
                value = buffer.getInt(i * BYTES_IN_INT);
              } else {
                storedFile.seek(i * BYTES_IN_INT);
                value = storedFile.readInt();
              }
              ret.fieldValues[i] = value;
            }
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
          return ret;
        }
      });
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    
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
