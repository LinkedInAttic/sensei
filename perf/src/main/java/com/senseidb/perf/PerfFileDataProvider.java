package com.senseidb.perf;

import java.io.File;
import java.util.Comparator;
import java.util.concurrent.LinkedBlockingQueue;

import org.json.JSONObject;

import proj.zoie.api.DataConsumer.DataEvent;

import com.senseidb.gateway.file.LinedJsonFileDataProvider;


public class PerfFileDataProvider extends LinedJsonFileDataProvider {

  public static LinkedBlockingQueue<JSONObject> queue;


  public PerfFileDataProvider(Comparator<String> versionComparator, File file, long startingOffset, LinkedBlockingQueue<JSONObject> queue) {
    super(versionComparator, file, startingOffset);
    this.queue = queue; 
  }
  
 
  
  
  @Override
  public DataEvent<JSONObject> next() {
    JSONObject object = null;
    try {
      object = queue.take();
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }    
    if (_offset % 10000 == 0) {
      System.out.println("Indexed " + _offset + " documents. Queue size = " + queue.size());
    }
    if (object != null) {
      return new DataEvent<JSONObject>(object, String.valueOf(_offset++));
    }
    
    return super.next();
  }

 
}
