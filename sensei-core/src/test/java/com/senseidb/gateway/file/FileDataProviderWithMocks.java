package com.senseidb.gateway.file;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;

import org.json.JSONException;
import org.json.JSONObject;

import proj.zoie.api.DataConsumer.DataEvent;


public class FileDataProviderWithMocks extends LinedJsonFileDataProvider {

  private LinkedBlockingQueue<JSONObject> queue;


  public FileDataProviderWithMocks(Comparator<String> versionComparator, File file, long startingOffset) {
    super(versionComparator, file, startingOffset);   
    queue = new LinkedBlockingQueue<JSONObject>(30000);
    mockRequests.add(queue); 
    instances.add(this);
  }
  public static List<FileDataProviderWithMocks> instances = new ArrayList<FileDataProviderWithMocks>();
  private static List<Queue<JSONObject>> mockRequests = new ArrayList<Queue<JSONObject>>();
  public static void resetOffset(long newOffset) {
    for (FileDataProviderWithMocks instance : instances) {
      instance._offset = newOffset;
    }
  }
  
  @Override
  public DataEvent<JSONObject> next() {
    JSONObject object = queue.poll();    
    if (object != null) {
      return new DataEvent<JSONObject>(object, String.valueOf(_offset++));
    }
    return super.next();
  }
  public static void add(JSONObject obj) {
    for (Queue<JSONObject> it : mockRequests) {
      it.add(clone(obj));
    }
  }
  private static JSONObject clone(JSONObject obj) {
     JSONObject ret = new JSONObject();
     for (String key : JSONObject.getNames(obj)) {
       try {
        ret.put(key, obj.opt(key));
      } catch (JSONException ex) {
        throw new RuntimeException(ex);
      }
     }
    return ret;
  }
  public long getOffset() {
    return _offset;
  }
}
