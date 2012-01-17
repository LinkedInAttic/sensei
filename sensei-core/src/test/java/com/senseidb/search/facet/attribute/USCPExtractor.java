package com.senseidb.search.facet.attribute;

import java.io.File;
import java.io.FileWriter;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.json.JSONArray;
import org.json.JSONObject;

public class USCPExtractor {
      public static void main(String[] args) throws Exception {
        String uscpStr = FileUtils.readFileToString(new File("uscp.txt"));
        JSONObject uscpJson = new JSONObject(uscpStr);
        JSONArray hits = uscpJson.getJSONArray("hits");
        File result = new File("uscp_data.json");
        if (result.exists()) result.delete();
        FileWriter fileWriter = new FileWriter(result, true);
        Set<String> obj_props = new HashSet<String>();
        for (int i = 0; i < hits.length(); i++) {
          JSONArray stored = hits.getJSONObject(i).getJSONArray("stored");
          String srcData = hits.getJSONObject(i).getString("srcdata");
          JSONObject ret = new JSONObject(srcData);
          for (int j = 0; j < stored.length(); j++) {
            JSONObject valJson = stored.optJSONObject(j);
            if (valJson == null) {
              continue;
            }
            String name = valJson.optString("name");
            String val = valJson.optString("val");
            
            if (name == null || val == null || name.startsWith("_")) {
              continue;
            }
            ret.put(name, val);
          }         
          
          //fileWriter.append(ret + "\n");
          if (ret.optString("object_property") != null && ret.optString("object_property").length() > 0) {
            obj_props.add(ret.optString("object_property"));
         }
        }
        System.out.println(obj_props);
      }
}
