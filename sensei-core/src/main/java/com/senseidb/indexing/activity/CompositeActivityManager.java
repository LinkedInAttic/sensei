package com.senseidb.indexing.activity;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.index.IndexReader;
import org.jboss.netty.util.internal.ConcurrentHashMap;
import org.json.JSONException;
import org.json.JSONObject;

import proj.zoie.api.Zoie;

import com.browseengine.bobo.api.BoboIndexReader;
import com.browseengine.bobo.facets.FacetHandler;
import com.senseidb.conf.SenseiConfParams;
import com.senseidb.conf.SenseiSchema;
import com.senseidb.conf.SenseiSchema.FacetDefinition;
import com.senseidb.conf.SenseiSchema.FieldDefinition;
import com.senseidb.indexing.ShardingStrategy;
import com.senseidb.indexing.activity.BaseActivityFilter.ActivityFilteredResult;
import com.senseidb.indexing.activity.facet.ActivityRangeFacetHandler;
import com.senseidb.indexing.activity.time.TimeAggregatedActivityValues;
import com.senseidb.plugin.SenseiPluginRegistry;
import com.senseidb.search.node.SenseiCore;
import com.senseidb.search.plugin.PluggableSearchEngine;


/**
 * Wrapper around all the activity indexes within the Sensei node. It is the entry point to the activity engine. 
 * AlsoProvides support to delete the docs that are purged from Zoie. 
 * This class is a bridge between Sensei and the activityEngine
 *
 */
public class CompositeActivityManager implements PluggableSearchEngine {
    
    protected CompositeActivityValues activityValues;
    private SenseiSchema senseiSchema;
    public static final String EVENT_TYPE_ONLY_ACTIVITY = "activity-update";
    private BaseActivityFilter activityFilter;
    private ShardingStrategy shardingStrategy;

    private SenseiCore senseiCore;
    private PurgeUnusedActivitiesJob purgeUnusedActivitiesJob;
    private SenseiPluginRegistry pluginRegistry; 
   
    
    
    public CompositeActivityManager() {
      
    }

   
    public String getVersion() {
      return activityValues.getVersion();
    }
   
    public boolean acceptEventsForAllPartitions() {
      if (activityFilter == null) 
        return false;
      return activityFilter.acceptEventsForAllPartitions();
    }
    public final void init(String indexDirectory, int nodeId, SenseiSchema senseiSchema, Comparator<String> versionComparator, SenseiPluginRegistry pluginRegistry, ShardingStrategy shardingStrategy) {
     
      this.senseiSchema = senseiSchema;
      this.pluginRegistry = pluginRegistry;
      this.shardingStrategy = shardingStrategy;
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
        activityValues = CompositeActivityValues.readFromFile(canonicalPath, fields, TimeAggregateInfo.valueOf(senseiSchema), versionComparator);
        activityFilter = pluginRegistry.getBeanByFullPrefix(SenseiConfParams.SENSEI_INDEX_ACTIVITY_FILTER, BaseActivityFilter.class);
        if (activityFilter == null) {
          activityFilter = new DefaultActivityFilter();
        }
        
        cachedInstances.put(nodeId, this);
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    }
    
    /**
     * Teels whether the document contains only activity fields. 
     * @param event
     * @return
     */
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
      return activityPresent && SenseiSchema.EVENT_TYPE_UPDATE.equalsIgnoreCase(event.optString(SenseiSchema.EVENT_TYPE_FIELD, null));
    }
    /**
     * Updates all the corresponding activity columns found in the document
     * @param event
     * @param version
     * @return 
     */
    public JSONObject acceptEvent(JSONObject event, String version) {
      try {        
        if (event.opt(SenseiSchema.EVENT_TYPE_SKIP) != null  ||  SenseiSchema.EVENT_TYPE_SKIP.equalsIgnoreCase(event.optString(SenseiSchema.EVENT_TYPE_FIELD))) {
          return event;
        }
        if (isOnlyActivityUpdate(event)) {
          event.put(SenseiSchema.EVENT_TYPE_FIELD, SenseiSchema.EVENT_TYPE_SKIP);
        }
        long defaultUid = event.getLong(senseiSchema.getUidField());       
        if (event.opt(SenseiSchema.EVENT_TYPE_FIELD) != null && event.optString(SenseiSchema.EVENT_TYPE_FIELD).equals(SenseiSchema.EVENT_TYPE_DELETE)) {
          activityValues.delete(defaultUid);
          return event;
        } 
        ActivityFilteredResult activityFilteredResult = activityFilter.filter(event, senseiSchema, shardingStrategy, senseiCore);
        for (long uid : activityFilteredResult.getActivityValues().keySet()) {          
          activityValues.update(uid, version, activityFilteredResult.getActivityValues().get(uid));
        }
        return activityFilteredResult.getFilteredObject();
      } catch (JSONException ex) {
        throw new RuntimeException(ex);
      }      
    }
    


    public CompositeActivityValues getActivityValues() {
      return activityValues;
    }
   
   
    protected static  Map<Integer, CompositeActivityManager> cachedInstances = new ConcurrentHashMap<Integer, CompositeActivityManager>();
    
    /**
     * Tells whether the activityEngine needs to be initialzed
     * @param schema
     * @return
     */
    public static boolean activitiesPresent(SenseiSchema schema) {
      for ( FieldDefinition field  : schema.getFieldDefMap().values()) {
        if (field.isActivity) {
          return true;
        }
      }
      return false;
    }


    /* (non-Javadoc)
     * @see com.senseidb.indexing.activity.deletion.DeletionListener#onDelete(org.apache.lucene.index.IndexReader, long[])
     */
    @Override
    public void onDelete(IndexReader indexReader, long... uids) {
      activityValues.delete(uids);  
    }
   public static class TimeAggregateInfo {
     public String fieldName;
     public List<String> times;     
     public TimeAggregateInfo(String fieldName, List<String> times) {
      this.fieldName = fieldName;
      this.times = times;
    }
     public TimeAggregateInfo() {
    }
     public static List<TimeAggregateInfo> valueOf(SenseiSchema senseiSchema) {
       List<TimeAggregateInfo> ret = new ArrayList<CompositeActivityManager.TimeAggregateInfo>();
       for (FacetDefinition facetDefinition : senseiSchema.getFacets()) {
         if ("aggregated-range".equals(facetDefinition.type)) {
           TimeAggregateInfo aggregateInfo = new TimeAggregateInfo();
           aggregateInfo.fieldName = facetDefinition.column;
           aggregateInfo.times = facetDefinition.params.get("time");
           ret.add(aggregateInfo);
         }
       }
       return ret;
     }
   }
 

  public void start(SenseiCore senseiCore) {
    this.senseiCore = senseiCore;
    Set<Zoie<BoboIndexReader,?>> zoieSystems = new HashSet<Zoie<BoboIndexReader,?>>();
    for (int partition : senseiCore.getPartitions()) {
      if (senseiCore.getIndexReaderFactory(partition) != null) {
        zoieSystems.add((Zoie<BoboIndexReader, ?>) senseiCore.getIndexReaderFactory(partition));
      }
    }
    purgeUnusedActivitiesJob = new PurgeUnusedActivitiesJob(activityValues, zoieSystems, PurgeUnusedActivitiesJob.extractFrequency(pluginRegistry));
    purgeUnusedActivitiesJob.start();
  }
  public void stop() {
    purgeUnusedActivitiesJob.stop();
    getActivityValues().flush();
    activityValues.close();
  }

 

  @Override
  public Set<String> getFieldNames() {
    Set<String> ret = new HashSet<String>();
    for (String field : senseiSchema.getFieldDefMap().keySet()) {
      if (senseiSchema.getFieldDefMap().get(field).isActivity) {
        ret.add(field);
      }
    }
    return ret;
  }
  @Override
  public Set<String> getFacetNames() {
    Set<String> ret = new HashSet<String>();
    for (FacetDefinition facet : senseiSchema.getFacets()) {
      boolean isActivity = facet.column != null && senseiSchema.getFieldDefMap().containsKey(facet.column) && senseiSchema.getFieldDefMap().get(facet.column).isActivity;
      boolean isAggregatedRange = "aggregated-range".equals(facet.type);
      if (isActivity || isAggregatedRange) {        
        ret.add(facet.name);
      }      
    }
    return ret;
  }


  @Override

  public List<FacetHandler<?>> createFacetHandlers() {
    Set<String> facets = getFacetNames();
    List<FacetHandler<?>> ret = new ArrayList<FacetHandler<?>>();
    for (FacetDefinition facet : senseiSchema.getFacets()) {
      if (!facets.contains(facet.name)) {        
        continue;
      }
      ActivityValues activityValues = getActivityValues().getActivityValuesMap().get(facet.column);

      if ("aggregated-range".equals(facet.type)) {
        if (!(activityValues instanceof TimeAggregatedActivityValues)) {
          throw new IllegalStateException("The facet " + facet.name + "should correspond to the timeAggregateActivityValues");          
        }
        TimeAggregatedActivityValues aggregatedActivityValues = (TimeAggregatedActivityValues) activityValues;
        for (String time : facet.params.get("time")) {
          String name = facet.name + ":" + time;
          ret.add(ActivityRangeFacetHandler.valueOf(name, facet.column, getActivityValues(), (ActivityIntValues)aggregatedActivityValues.getValuesMap().get(time)));
        }
        ret.add(ActivityRangeFacetHandler.valueOf(facet.name, facet.column, getActivityValues(), (ActivityIntValues)aggregatedActivityValues.getDefaultIntValues()));
      } else if ("range".equals(facet.type)){
        ret.add(ActivityRangeFacetHandler.valueOf(facet.name, facet.column, getActivityValues(), getActivityValues().getActivityIntValues(facet.column)));
      } else {
        throw new UnsupportedOperationException("The facet " + facet.name + "should be of type either aggregated-range or range");
      }
    }
    return ret;
  }


  public PurgeUnusedActivitiesJob getPurgeUnusedActivitiesJob() {
    return purgeUnusedActivitiesJob;
  }
  
}
