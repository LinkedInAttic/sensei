package com.senseidb.indexing.activity;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
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
    private Comparator<String> versionComparator; 
    public CompositeActivityManager() {
      
    }
    public CompositeActivityManager(String indexDirectory, int nodeId, SenseiSchema senseiSchema, Comparator<String> versionComparator) {
      this.init(indexDirectory, nodeId, senseiSchema, versionComparator);
    }
    
    public String getOldestSinceVersion() {
      String ret = null;
      for (ActivityFieldValues activityFieldValues : columnsMap.values()) {
        if (ret != null && versionComparator.compare(activityFieldValues.getVersion(), ret) < 0) {
          ret = activityFieldValues.getVersion();
        }
        else {
          ret = activityFieldValues.getVersion();;
        }
      }
      return ret;
    }
    
    public final void init(String indexDirectory, int nodeId, SenseiSchema senseiSchema, Comparator<String> versionComparator) {
      this.nodeId = nodeId;
      this.senseiSchema = senseiSchema;
      this.versionComparator = versionComparator;
      columnsMap = Collections.synchronizedMap(new HashMap<String, ActivityFieldValues>());
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
    public ActivityFieldValues getActivityFieldValues(String field) {
      return columnsMap.get(field);
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
