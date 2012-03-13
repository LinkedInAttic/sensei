package com.senseidb.indexing.activity;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.lucene.index.IndexReader;
import org.jboss.netty.util.internal.ConcurrentHashMap;
import org.json.JSONException;
import org.json.JSONObject;

import com.senseidb.conf.SenseiSchema;
import com.senseidb.conf.SenseiSchema.FieldDefinition;
import com.senseidb.indexing.activity.deletion.DeletionListener;

public class CompositeActivityManager implements DeletionListener {
    protected CompositeActivityValues activityValues;
    private SenseiSchema senseiSchema;
    public static final String EVENT_TYPE_ONLY_ACTIVITY = "activity-update";
    private int nodeId;
   
    
    
    public CompositeActivityManager() {
      
    }
    public CompositeActivityManager(String indexDirectory, int nodeId, SenseiSchema senseiSchema, Comparator<String> versionComparator) {
      this.init(indexDirectory, nodeId, senseiSchema, versionComparator);
    }
    
    public String getOldestSinceVersion() {
      return activityValues.getVersion();
    }
    
    public final void init(String indexDirectory, int nodeId, SenseiSchema senseiSchema, Comparator<String> versionComparator) {
      this.nodeId = nodeId;
      this.senseiSchema = senseiSchema;
      try {
        File dir = new File(indexDirectory, "node" +nodeId +"/activity");
        dir.mkdirs();
        String canonicalPath = dir.getCanonicalPath();
        List<String> fields = new ArrayList<String>();
        for ( String field  : senseiSchema.getFieldDefMap().keySet()) {
          FieldDefinition fieldDefinition = senseiSchema.getFieldDefMap().get(field);
          if (fieldDefinition.isActivity) {
            fields.add(field);
          }
        }
        activityValues = CompositeActivityValues.readFromFile(canonicalPath, fields, versionComparator);
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
        if (event.opt(SenseiSchema.EVENT_TYPE_FIELD) != null && event.optBoolean(SenseiSchema.EVENT_TYPE_FIELD) == Boolean.TRUE) {
          activityValues.delete(uid);
        } else {
          activityValues.update(uid, version, event);
        }
      } catch (JSONException ex) {
        throw new RuntimeException(ex);
      }      
    }
    
    
    public CompositeActivityValues getActivityValues() {
      return activityValues;
    }
    public void close() {
      activityValues.close();
    }
   
    protected static  Map<Integer, CompositeActivityManager> cachedInstances = new ConcurrentHashMap<Integer, CompositeActivityManager>();
    public static boolean activitiesPresent(SenseiSchema schema) {
      for ( FieldDefinition field  : schema.getFieldDefMap().values()) {
        if (field.isActivity) {
          return true;
        }
      }
      return false;
    }


    @Override
    public void onDelete(IndexReader indexReader, long... uids) {
      activityValues.delete(uids);  
    }
   
}
