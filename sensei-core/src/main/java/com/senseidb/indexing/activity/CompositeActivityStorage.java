package com.senseidb.indexing.activity;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.springframework.util.Assert;

import com.senseidb.indexing.activity.primitives.ActivityPrimitivesStorage;
import com.senseidb.metrics.MetricsConstants;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.Timer;

public class CompositeActivityStorage {
  private static final int BYTES_IN_LONG = 8;
  private static Logger logger = Logger.getLogger(CompositeActivityStorage.class);
  private RandomAccessFile storedFile;  
  private final String indexDir;
  private volatile boolean closed = false;
  private MappedByteBuffer buffer;
  private long fileLength; 
  private boolean activateMemoryMappedBuffers = false;
  private Timer timer;

  public CompositeActivityStorage(String indexDir) {   
    this.indexDir = indexDir;
    timer = Metrics.newTimer(new MetricName(MetricsConstants.Domain,"timer","initCompositeActivities-time" ,"CompositeActivityStorage"), TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
 
  }

  public synchronized void init() {
    try {
      File dir = new File(indexDir);
      if (!dir.exists()) {
        dir.mkdirs();
      }
      File file = new File(dir,  "activity.indexes");
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
         ensureCapacity(update.index * BYTES_IN_LONG + BYTES_IN_LONG);        
         if (activateMemoryMappedBuffers) {
           buffer.putLong(update.index * BYTES_IN_LONG, update.value);
         } else {
           storedFile.seek(update.index * BYTES_IN_LONG);
           storedFile.writeLong(update.value); 
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
   
    if (fileLength > ActivityPrimitivesStorage.LENGTH_THRESHOLD) {
      fileLength = fileLength * ActivityPrimitivesStorage.FILE_GROWTH_RATIO;
    } else {
      fileLength = ActivityPrimitivesStorage.INITIAL_FILE_LENGTH;
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

  public void decorateCompositeActivityValues(final CompositeActivityValues activityValues, final Metadata metadata) {
    try {
      timer.time(new Callable<CompositeActivityValues>() {

        @Override
        public CompositeActivityValues call() throws Exception {
          Assert.state(storedFile != null, "The FileStorage is not initialized");         
          activityValues.activityStorage = CompositeActivityStorage.this;
          try {
            if (metadata.count == 0) {
              activityValues.init();
              return activityValues;
            }
            activityValues.init((int) (metadata.count * ActivityPrimitivesStorage.INIT_GROWTH_RATIO));
            synchronized (activityValues.deletedIndexes) {
              if (metadata.count * BYTES_IN_LONG > fileLength) {
                logger.warn("The composite activityIndex is corrupted. The file contains " + (fileLength / BYTES_IN_LONG) + " records, while metadata a bigger number " + metadata.count);
                logger.warn("trimming the metadata");
                int newCount = (int)(fileLength / BYTES_IN_LONG);
                metadata.update(metadata.version, newCount);
              }
              for (int i = 0; i < metadata.count; i++) {
                long value;
                if (activateMemoryMappedBuffers) {
                  value = buffer.getLong(i * BYTES_IN_LONG);
                } else {
                  storedFile.seek(i * BYTES_IN_LONG);
                  value = storedFile.readLong();
                }           
               
                
                if (value != Long.MIN_VALUE) {
                  activityValues.uidToArrayIndex.put(value, i);
                } else {
                  activityValues.deletedIndexes.add(i);
                }
              }
            }
            activityValues.indexSize = new AtomicInteger(activityValues.uidToArrayIndex.size() + activityValues.deletedIndexes.size());
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
          return activityValues;
        }
      });
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
   
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
