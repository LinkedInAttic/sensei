package com.sensei.facet.attributes;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;

public class DataConverter {
    private static Map<String, Integer> keys = new LinkedHashMap<String, Integer>();
    public static int numOfElementsPerDoc = 3;
    static {
      keys.put("key1", 200);
      keys.put("key2", 190);
      keys.put("key3", 180);
      keys.put("key4", 170);
      keys.put("key5", 50);
      keys.put("key6", 30);
      keys.put("key7", 25);
      keys.put("key8", 15);
      keys.put("key9", 10);
      keys.put("key10", 5);
    }
    static List<String> values = new ArrayList<String>(100);
    static Iterator<String> iterator;
    public static String next() {
      if (iterator == null || !iterator.hasNext()) {
        iterator = values.iterator();
      }
      return iterator.next();
    }
    
    
    public static void main(String[] args) throws Exception {
      values = createValuesList(keys);
      File carsFile = new File("../example/cars/data/cars.json");
      File resultFile = new File("test_data.json");
      FileWriter fileWriter = new FileWriter(resultFile, true);
      
      for (String line : IOUtils.readLines(new FileInputStream(carsFile))) {
        line = line.replaceFirst(",", formDocEntry(numOfElementsPerDoc));
        fileWriter.append(line + "\n");
      }
      fileWriter.close();
      
      String value  = formDocEntry(numOfElementsPerDoc);
      
    }
    private static String formDocEntry(int numOfElementsPerDoc) {
      String ret = ",\"object_properties\":\"";
      for (int i = 0; i < numOfElementsPerDoc - 1; i++) {
        ret += next() + ",";
      }
      ret += next() + "\",";
      return ret;
    }


    private static List<String> createValuesList(Map<String, Integer> keys) {
      List<String> ret = new ArrayList<String>(keys.size() * 10);
      for(String key : keys.keySet()) {
        int frequency = keys.get(key);
        for (int i = 1; i < 11; i++) {
          String val = "Value" + i;
          for (int j = 0; j < frequency; j++) {
            ret.add(key + "=" + val);
          }
          frequency = (int)(frequency * 0.85);          
          if (frequency < 1) {
            frequency = 1;
          }
        }
      }
      System.out.println(ret);
      Collections.shuffle(ret);
      return ret;
    }
}
