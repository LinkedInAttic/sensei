package com.senseidb.indexing.activity;

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
  private File metadataFile;
  private final String fieldName;
  private final String indexDir;
  private volatile boolean closed = false;
  private MappedByteBuffer buffer;
  private long fileLength;
  private Metadata metadata;
  public FileStorage(String fieldName, String indexDir) {
    this.fieldName = fieldName;
    this.indexDir = indexDir;
  }

  public synchronized void init() {
    try {
      File file = new File(indexDir, fieldName + ".data");
      metadataFile = new File(indexDir, fieldName + ".metadata");
      if (!file.exists()) {
        file.createNewFile();
      }
      storedFile = new RandomAccessFile(file, "rw");
      fileLength = storedFile.length();
       buffer = storedFile.getChannel().map(MapMode.READ_WRITE, 0, file.length());
      if (!metadataFile.exists()) {
        metadataFile.createNewFile();
      } else {
        String versionPlusCapacity = FileUtils.readFileToString(metadataFile);
        metadata = Metadata.valueOf(versionPlusCapacity);       
        
      }
    } catch (IOException e) {
      logger.error(e.getMessage(), e);
      throw new RuntimeException(e);
    }
  }

  public synchronized void applyUpdates(List<FieldUpdate> updates) {
    Assert.state(metadataFile != null, "The FileStorage is not initialized");    
    try {
      for (FieldUpdate update : updates) {
        //storedFile.seek(update.index * 12);
       // storedFile.writeLong(update.uid);
        //storedFile.seek(update.index * 12 + 8);
        //storedFile.writeLong(update.value);
         ensureCapacity(update.index * 12 + 8);
         buffer.putLong(update.index * 12, update.uid);
         buffer.putInt(update.index * 12 + 8, update.value);
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
    buffer = storedFile.getChannel().map(MapMode.READ_WRITE, 0,  fileLength);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public synchronized void commit(String version, int count) {
    try {
      
      buffer.force();
      //storedFile.getFD().sync();     
      FileUtils.writeStringToFile(metadataFile, version + ";" + count);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
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
    Assert.state(metadataFile != null, "The FileStorage is not initialized");
    ActivityFieldValues ret = new ActivityFieldValues();
    ret.versionComparator = versionComparator;    
    ret.activityFieldStore = this;
    ret.fieldName = fieldName;
    try { 
    if (metadata == null) {
      ret.init();
      ret.lastVersion = "";
      ret.lastPersistedVersion = "";    
      return ret;
    }    
    ret.init((int)(metadata.count * 1.5));
    int fileIndex = 0;    
      for (int i = 0; i < metadata.count; i ++) {
        ret.uidToArrayIndex.put(buffer.getLong((int) fileIndex), i);
        ret.fieldValues[i] = buffer.getInt(fileIndex + 8);        
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
    public Metadata(String version, Integer count) {
      super();
      this.version = version;
      this.count = count;
    }
    @Override
    public String toString() {
     return version + ";" + count;
    }
    public static Metadata valueOf(String str) {
      if (!str.contains(";")) {
        return null;
      }
      String version = str.split(";")[0];
      int actualCapacity = Integer.parseInt(str.split(";")[1]);
      return new Metadata(version, actualCapacity);
    }
  }

}
