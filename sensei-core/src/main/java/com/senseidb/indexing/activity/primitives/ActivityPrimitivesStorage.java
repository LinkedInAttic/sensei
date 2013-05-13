package com.senseidb.indexing.activity.primitives;

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

import com.senseidb.indexing.activity.AtomicFieldUpdate;
import com.senseidb.metrics.MetricsConstants;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.Timer;

/**
 * Allows to persist ActivityIntValues into the file. The persistence is asynchronous via {@link ActivityIntValues#prepareFlush()}
 *
 */
public class ActivityPrimitivesStorage {
  public static final double INIT_GROWTH_RATIO = 1.5;
  //public static final int BYTES_IN_INT = 4;
  public static final int LENGTH_THRESHOLD = 1000000;
  public static final int FILE_GROWTH_RATIO = 2;
  public static final int INITIAL_FILE_LENGTH = 2000000;
  private static Logger logger = Logger.getLogger(ActivityPrimitivesStorage.class);
  private RandomAccessFile storedFile;
  protected final String fieldName;
  private final String indexDir;
  private volatile boolean closed = false;
  private MappedByteBuffer buffer;
  private long fileLength; 
  private boolean activateMemoryMappedBuffers = true;
  private static Timer timer;
  private String fileName;
  
  public ActivityPrimitivesStorage(String fieldName, String indexDir) {
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

  public synchronized void flush(List<AtomicFieldUpdate> updates) {
    Assert.state(storedFile != null, "The FileStorage is not initialized");    
    try {
      for (AtomicFieldUpdate update : updates) {       
         ensureCapacity((update.index + 1) * update.getFieldSizeInBytes());        
         if (activateMemoryMappedBuffers) {
           update.update(buffer, update.index * update.getFieldSizeInBytes());          
         } else {  
           update.update(storedFile, update.index * update.getFieldSizeInBytes());
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
  private void ensureCapacity(int i) {
    try {
      if (fileLength > i + 100) {
        return;
      }
      if (fileLength < INITIAL_FILE_LENGTH) {
        fileLength = INITIAL_FILE_LENGTH;
      }
      while (fileLength < i + 100) {
        fileLength = fileLength * FILE_GROWTH_RATIO;
      }
      storedFile.setLength(fileLength);
      if (activateMemoryMappedBuffers) {
        buffer = storedFile.getChannel().map(MapMode.READ_WRITE, 0, fileLength);
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

  protected  void initActivityDataFromFile(final ActivityPrimitiveValues activityPrimitiveValues, final int count) {
    try {
      timer.time(new Callable<ActivityPrimitiveValues>() {
        @Override
        public ActivityPrimitiveValues call() throws Exception {
          Assert.state(storedFile != null, "The FileStorage is not initialized");        
          activityPrimitiveValues.activityFieldStore = ActivityPrimitivesStorage.this;
          activityPrimitiveValues.fieldName = fieldName;
          try {
            if (count == 0) {
              activityPrimitiveValues.init();
              return activityPrimitiveValues;
            }
            activityPrimitiveValues.init((int) (count * INIT_GROWTH_RATIO));
            if (fileLength < count * activityPrimitiveValues.getFieldSizeInBytes()) {
              logger.warn("The  activityIndex is corrupted. The file "+ fieldName +" contains " + (fileLength / activityPrimitiveValues.getFieldSizeInBytes()) + " records, while metadata has a bigger number " + count);
              logger.warn("adding extra space");
              ensureCapacity(count * activityPrimitiveValues.getFieldSizeInBytes());
            }
            if (activateMemoryMappedBuffers) {
              activityPrimitiveValues.initFieldValues(count, buffer);
            } else {
              activityPrimitiveValues.initFieldValues(count, storedFile);
            }
            
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
          return activityPrimitiveValues;
        }
      });
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    
  }
  
  public boolean isClosed() {
    return closed;
  }  

  
}
