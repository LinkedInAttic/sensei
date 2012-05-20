package com.senseidb.gateway.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;

import junit.framework.TestCase;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.json.JSONObject;
import org.junit.BeforeClass;

import com.linkedin.zoie.api.DataConsumer;
import com.linkedin.zoie.api.ZoieException;
import com.linkedin.zoie.api.DataConsumer.DataEvent;
import com.linkedin.zoie.impl.indexing.StreamDataProvider;
import com.linkedin.zoie.impl.indexing.ZoieConfig;

import com.senseidb.gateway.SenseiGateway;
import com.senseidb.plugin.SenseiPluginRegistry;

public class BaseGatewayTestUtil {
  static File dataFile = new File("src/test/resources/test.json");
  
  
  
  static List<JSONObject> dataList;

  static {
    try {
      dataList = readData(dataFile);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
  
  static List<JSONObject> readData(File file) throws Exception {
    LinkedList<JSONObject> dataList = new LinkedList<JSONObject>();
    BufferedReader reader = new BufferedReader(new FileReader(file));
    while (true) {
      String line = reader.readLine();
      if (line == null)
        break;
      dataList.add(new JSONObject(line));
    }
    return dataList;
  }
  

  public static void compareResultList(List<JSONObject> jsonList) throws Exception{
    for (int i =0;i<jsonList.size();++i){
      String s1 = jsonList.get(i).getString("id");
      String s2 = BaseGatewayTestUtil.dataList.get(i).getString("id");
      TestCase.assertEquals(s1, s2);
    }
  }
  
  public static void doTest(StreamDataProvider<JSONObject> dataProvider) throws Exception{
    final LinkedList<JSONObject> jsonList = new LinkedList<JSONObject>();
    
    dataProvider.setDataConsumer(new DataConsumer<JSONObject>(){

      private volatile String version;
      @Override
      public void consume(
          Collection<DataEvent<JSONObject>> events)
          throws ZoieException {

        for (DataEvent<JSONObject> event : events){
          JSONObject jsonObj = event.getData();
          System.out.println(jsonObj+", version: "+event.getVersion());
          jsonList.add(jsonObj);
          version = event.getVersion();
        }
       
      }

      @Override
      public String getVersion() {
        return version;
      }

      @Override
      public Comparator<String> getVersionComparator() {
        return ZoieConfig.DEFAULT_VERSION_COMPARATOR;
      }
      
    });
    dataProvider.start();
    
    while(true){
      Thread.sleep(500);
      if (jsonList.size()==BaseGatewayTestUtil.dataList.size()){
        dataProvider.stop();
        BaseGatewayTestUtil.compareResultList(jsonList);
        break;
      }
    }
  }

}
