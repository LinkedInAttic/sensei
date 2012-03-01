package com.senseidb.indexing.activity;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntList;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.springframework.util.Assert;

import com.senseidb.conf.SenseiServerBuilder;

public class FileStorage {
  private static Logger logger = Logger.getLogger(FileStorage.class);
  private RandomAccessFile storedFile;
  private final String fieldName;
  private final String indexDir;
  private volatile boolean closed = false;
  private MappedByteBuffer buffer;
  private long fileLength;
  private Metadata metadata;
  protected IntList pendingDeletions = new IntArrayList(2000);
  
  
  public FileStorage(String fieldName, String indexDir) {
    this.fieldName = fieldName;
    this.indexDir = indexDir;
  }

  public synchronized void init() {
    try {
      File file = new File(indexDir, fieldName + ".data");
      if (!file.exists()) {
        file.createNewFile();
      }
      storedFile = new RandomAccessFile(file, "rw");
      fileLength = storedFile.length();
       buffer = storedFile.getChannel().map(MapMode.READ_WRITE, 0, file.length());
     
        metadata = new Metadata(indexDir, fieldName);      
        metadata.init();
      
    } catch (IOException e) {
      logger.error(e.getMessage(), e);
      throw new RuntimeException(e);
    }
  }

  public synchronized void applyUpdates(List<FieldUpdate> updates) {
    Assert.state(metadata != null, "The FileStorage is not initialized");    
    try {
      for (FieldUpdate update : updates) {       
         ensureCapacity(update.index * 12 + 8);
         buffer.putLong(update.index * 12, update.uid);
         buffer.putInt(update.index * 12 + 8, update.value);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
  public synchronized void delete(IntList indexesToDelete) {   
    for (int index : indexesToDelete) {
      buffer.putInt(index * 12 + 8, Integer.MIN_VALUE);
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
    buffer = storedFile.getChannel().map(MapMode.READ_WRITE, 0,  fileLength);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public synchronized void commit(String version, int count) { 
      buffer.force();
      metadata.update(version, count);
  }
  public synchronized void close() {
    try {
      storedFile.close();
      closed = true;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
  public ActivityFieldValues getActivityDataFromFile(Comparator<String> versionComparator) {
    Assert.state(metadata != null, "The FileStorage is not initialized");
    ActivityFieldValues ret = new ActivityFieldValues();
    ret.versionComparator = versionComparator;    
    ret.activityFieldStore = this;
    ret.fieldName = fieldName;
    try { 
    if (metadata == null || metadata.count == null) {
      ret.init();
      ret.lastVersion = "";
      ret.lastPersistedVersion = "";    
      return ret;
    }    
    ret.init((int)(metadata.count * 1.5));
    ret.arraySize = metadata.count;
    int fileIndex = 0;    
      for (int i = 0; i < metadata.count; i ++) {
        int value = buffer.getInt(fileIndex + 8);
        //deleted docs
        if (value == Integer.MIN_VALUE) {
          ret.deletedIndexes.add(i);
          ret.fieldValues[i] = Integer.MIN_VALUE; 
          fileIndex += 12;
          continue;
        }
        ret.uidToArrayIndex.put(buffer.getLong((int) fileIndex), i);
        ret.fieldValues[i] = value; 
        
        fileIndex += 12;
      }    
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    ret.lastVersion = metadata.version;
    ret.lastPersistedVersion = metadata.version;
    return ret;
  }
  
  public boolean isClosed() {
    return closed;
  }  

  public static class FieldUpdate {
    public int index;
    public long uid;
    public int value;
    public String version;

    public FieldUpdate(int index, long uid, int value, String version) {
      this.index = index;
      this.uid = uid;
      this.value = value;
      this.version = version;
    }

    @Override
    public String toString() {
      return "index=" + index + ", uid=" + uid + ", value=" + value + ", version=" + version ;
    }
    
  }

  public static class UpdateBatch {
    int batchSize = 5000;
    protected volatile List<FieldUpdate> updates = new ArrayList<FileStorage.FieldUpdate>(batchSize);
    long time = System.currentTimeMillis();

    public boolean addFieldUpdate(FieldUpdate fieldUpdate) {
      updates.add(fieldUpdate);
      if (updates.size() == batchSize || (System.currentTimeMillis() - time) > 60 * 1000) {
        return true;
      }
      return false;
    }

    public List<FieldUpdate> getUpdates() {
      return updates;
    }
  }
  public static class Metadata {
    public String version;
    public Integer count;
    private final String indexDir;
    private final String fieldName;
    private File file1;
    private File file2;
    public Metadata(String indexDir, String fieldName) {
      super();
      this.indexDir = indexDir;
      this.fieldName = fieldName;      
    }
    public void init() throws IOException {
      file1 = new File(indexDir, fieldName + ".metadata1");
      file2 = new File(indexDir, fieldName + ".metadata2");
      if (!file1.exists()) {
        file1.createNewFile();
      }
      if (!file2.exists()) {
        file2.createNewFile();
      } else {
        long modifiedTime1 = file1.lastModified();
        long modifiedTime2 = file2.lastModified();
        if (modifiedTime1 > modifiedTime2) {
          init(FileUtils.readFileToString(file2));
        } else {
          init(FileUtils.readFileToString(file1));
        }
      }      
    }
    public void update(String version, int count)  {
      this.version = version;
      this.count = count;
      try {
      FileUtils.writeStringToFile(file1, this.toString());
      FileUtils.writeStringToFile(file2, this.toString());
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    }
    @Override
    public String toString() {
     return version + ";" + count;
    }
    protected void init(String str) {
      if (!str.contains(";")) {
        return ;
      }
      version = str.split(";")[0];
      count = Integer.parseInt(str.split(";")[1]);     
    }
  }
  
 

}
