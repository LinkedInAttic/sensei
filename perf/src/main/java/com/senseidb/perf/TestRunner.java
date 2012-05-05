package com.senseidb.perf;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.lucene.search.Sort;
import org.json.JSONException;
import org.json.JSONObject;

import com.senseidb.search.client.SenseiServiceProxy;
import com.senseidb.search.client.json.JsonSerializer;
import com.senseidb.search.client.req.SenseiClientRequest;
import com.senseidb.search.node.SenseiServer;


public class TestRunner {
  static  int generatedUid = 0;
  private static ArrayList<JSONObject> jsons;
  private static int size;
   static int readUid = 0;
   
  public static void main(String[] args) throws Exception {
    org.apache.log4j.PropertyConfigurator.configure("conf-perf/log4j-perf.properties");
    SenseiServer.main(new String[]{"conf-perf"});
      List<String> linesFromFile = FileUtils.readLines(new File("data/cars.json"));
      jsons = new ArrayList<JSONObject>();
      for (String line : linesFromFile) {
        if (line == null || !line.contains("{")) {
          continue;
        }
        jsons.add(new JSONObject(line));
      }
      size = jsons.size();
      Thread thread = new Thread() {
        public void run() {
          while(true) {
            putNextDoc();
           
          }
        };
      };
      thread.start();  
      Thread[] queryThreads = new Thread[1];
      final SenseiServiceProxy proxy = new SenseiServiceProxy("localhost", 8080);
      Runnable query = new Runnable() {
        
        @Override
        public void run() {
          while (true) {
            String sendPostRaw = proxy.sendPostRaw(proxy.getSearchUrl(),  ((JSONObject)JsonSerializer.serialize(SenseiClientRequest.builder().addSort(com.senseidb.search.client.req.Sort.desc("mileage")).build())).toString());
            try {
              int numihits = new JSONObject(sendPostRaw).getInt("totaldocs");
              //System.out.println(numihits);
              Thread.sleep(500);
              if (numihits == 0) {
                System.out.println("!!!!numihits is 0");
                
                //System.exit(0);
              }
            } catch (Exception e) {
              // TODO Auto-generated catch block
              e.printStackTrace();
            }
          }
          
        }
      };
      for (int i = 0; i < queryThreads.length; i++) {
        queryThreads[i] = new Thread(query);
        queryThreads[i].start();
      }
      Thread.sleep(60 * 60 * 60 * 1000);
      thread.join();
  }
  public static void putNextDoc() {
    if (readUid == size) {
      readUid = 0;
    }
    if (generatedUid == 30000000) {
      generatedUid = 0;
    }
    JSONObject newEvent = clone(jsons.get(readUid++));
    try {
      newEvent.put("id", generatedUid++);
      PerfFileDataProvider.queue.put(newEvent);
      //System.out.println("put doc = " + generatedUid);
    } catch (Exception  e) {
      System.out.println("Error " + e.getMessage());
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
}
