package com.senseidb.indexing.activity;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.jboss.netty.util.internal.ConcurrentHashMap;
import org.json.JSONException;
import org.json.JSONObject;

import com.senseidb.conf.SenseiSchema;
import com.senseidb.conf.SenseiSchema.FieldDefinition;

public class CompositeActivityManager {
    protected Map<String, ActivityFieldValues> columnsMap;
    private SenseiSchema senseiSchema;
    public static final String EVENT_TYPE_ONLY_ACTIVITY = "activity-update";
    private int nodeId; 
    public void init(String indexDirectory, int nodeId, SenseiSchema senseiSchema, Comparator<String> versionComparator) {
      this.nodeId = nodeId;
      this.senseiSchema = senseiSchema;
      columnsMap = new HashMap<String, ActivityFieldValues>();
      try {
        File dir = new File(indexDirectory, "node" +nodeId +"/activity");
        dir.mkdirs();
        String canonicalPath = dir.getCanonicalPath();
      for (Entry<String,FieldDefinition> entry : senseiSchema.getFieldDefMap().entrySet()) {
        if (entry.getValue().isActivity) {
          columnsMap.put(entry.getKey(), ActivityFieldValues.readFromFile(canonicalPath, entry.getKey(), versionComparator));
        }
      }
      cachedInstances.put(nodeId, this);
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    }
    public boolean isOnlyActivityUpdate(JSONObject event) {
      boolean activityPresent = false;     
      Iterator keys = event.keys();
      while (keys.hasNext()) {
        String key = (String) keys.next();
        FieldDefinition fieldDefinition = senseiSchema.getFieldDefMap().get(key);
        if (fieldDefinition == null || senseiSchema.getUidField().equals(key)) {
          continue;
        }
        if (fieldDefinition.isActivity) {
          activityPresent = true;
        } else {
          return false;
        }       
      }
      return activityPresent;
    }
    public void update(JSONObject event, String version) {
      try {
        if (event.opt(SenseiSchema.EVENT_TYPE_SKIP) != null && event.opt(EVENT_TYPE_ONLY_ACTIVITY) == null) {
          return;
        }        
        long uid = event.getLong(senseiSchema.getUidField());
        //System.out.println("!!!setValue uid = " + uid + ", value = " + event.optString("likes") + ", threadId=" + Thread.currentThread().getId() + ",nodeId=" + nodeId);
        for (String field : columnsMap.keySet()) {
          columnsMap.get(field).update(uid, version, event.opt(field));
          event.remove(field);
        }
      } catch (JSONException ex) {
        throw new RuntimeException(ex);
      }      
    }
    public void close() {
      for (ActivityFieldValues activityFieldValues : columnsMap.values()) {
        activityFieldValues.close();
      }
    }
   
    protected static  Map<Integer, CompositeActivityManager> cachedInstances = new ConcurrentHashMap<Integer, CompositeActivityManager>();
}
