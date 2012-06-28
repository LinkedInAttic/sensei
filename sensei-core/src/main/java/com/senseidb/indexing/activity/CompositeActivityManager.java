package com.senseidb.indexing.activity;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.lucene.index.IndexReader;
import org.jboss.netty.util.internal.ConcurrentHashMap;
import org.json.JSONException;
import org.json.JSONObject;

import proj.zoie.api.IndexReaderFactory;
import proj.zoie.api.Zoie;
import proj.zoie.api.ZoieIndexReader;
import proj.zoie.api.ZoieMultiReader;
import proj.zoie.api.ZoieSegmentReader;
import scala.actors.threadpool.Arrays;

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
import com.senseidb.search.plugin.PluggableSearchEngineManager;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.MetricName;


/**
 * Wrapper around all the activity indexes within the Sensei node. It is the entry point to the activity engine. 
 * AlsoProvides support to delete the docs that are purged from Zoie. 
 * This class is a bridge between Sensei and the activityEngine
 *
 */
public class CompositeActivityManager implements PluggableSearchEngine {
    private final static Logger logger = Logger.getLogger(PluggableSearchEngineManager.class);
    protected CompositeActivityValues activityValues;
    private SenseiSchema senseiSchema;
    public static final String EVENT_TYPE_ONLY_ACTIVITY = "activity-update";
    private BaseActivityFilter activityFilter;
    private ShardingStrategy shardingStrategy;
    private SenseiCore senseiCore;
    private PurgeUnusedActivitiesJob purgeUnusedActivitiesJob;
    private SenseiPluginRegistry pluginRegistry; 
    private Map<String, Set<String>> columnToFacetMapping = new HashMap<String, Set<String>>();
    private Counter recoveredIndexInBoboFacetDataCache;
    private Counter facetMappingMismatch;
    
    public CompositeActivityManager() {
      recoveredIndexInBoboFacetDataCache = Metrics.newCounter(new MetricName(getClass(), "recoveredIndexInBoboFacetDataCache"));
      facetMappingMismatch = Metrics.newCounter(new MetricName(getClass(), "facetMappingMismatch"));
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
        initColumnFacetMapping(senseiSchema);
        cachedInstances.put(nodeId, this);
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    }
    private void initColumnFacetMapping(SenseiSchema senseiSchema) {
      Set<String> facetNames = getFacetNames();
      for (FacetDefinition facet : senseiSchema.getFacets()) {
        if (facet.name == null || facet.column == null || !facetNames.contains(facet.name)) {
          continue;
        }
        if (!columnToFacetMapping.containsKey(facet.column)) {
          columnToFacetMapping.put(facet.column, new HashSet<String>());
        }
        if ("aggregated-range".equals(facet.type) && facet.params.containsKey("time")) {
          for (String time : facet.params.get("time")) {
            String name = facet.name + ":" + time;
            columnToFacetMapping.get(facet.column).add(name);
          }
        } 
        columnToFacetMapping.get(facet.column).add(facet.name);
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
        boolean onlyActivityUpdate = isOnlyActivityUpdate(event);
        if (onlyActivityUpdate) {
          event.put(SenseiSchema.EVENT_TYPE_FIELD, SenseiSchema.EVENT_TYPE_SKIP);
        }
        long defaultUid = event.getLong(senseiSchema.getUidField());       
        if (event.opt(SenseiSchema.EVENT_TYPE_FIELD) != null && event.optString(SenseiSchema.EVENT_TYPE_FIELD).equals(SenseiSchema.EVENT_TYPE_DELETE)) {
          activityValues.delete(defaultUid);
          return event;
        } 
        ActivityFilteredResult activityFilteredResult = activityFilter.filter(event, senseiSchema, shardingStrategy, senseiCore);
        onlyActivityUpdate = onlyActivityUpdate || activityFilteredResult.getFilteredObject() == null || activityFilteredResult.getFilteredObject().length() == 0 ||SenseiSchema.EVENT_TYPE_SKIP.equals(activityFilteredResult.getFilteredObject().optBoolean(SenseiSchema.EVENT_TYPE_FIELD));
        for (long uid : activityFilteredResult.getActivityValues().keySet()) {
          if ( activityFilteredResult.getActivityValues().get(uid) == null || activityFilteredResult.getActivityValues().get(uid).size() == 0) {
            continue;
          }
          int previousIndex = activityValues.getIndexByUID(uid);
          int index = activityValues.update(uid, version, activityFilteredResult.getActivityValues().get(uid));
          if (index >= 0 && previousIndex < 0 && (onlyActivityUpdate || defaultUid != uid)) {
            updateExistingBoboIndexes(uid, index, activityFilteredResult.getActivityValues().get(uid).keySet());
          }
        }
        return activityFilteredResult.getFilteredObject();
      } catch (JSONException ex) {
        throw new RuntimeException(ex);
      }      
    }

    /** Handles the specific case, when we've received a document, that will cause a creation of the new activityValue, but the corresponding document in Zoie remains untouched. 
     * We have to manually update Bobo indexes in this case
     * @param uid
     * @param index
     * @param columns
     */
    private void updateExistingBoboIndexes(long uid, int index, Set<String> columns) {
      if (columns.isEmpty()) {
        return;
      }
      Set<String> facets = new HashSet<String>();
      for (String column : columns) {
        if (columnToFacetMapping.containsKey(column)) {
          facets.addAll(columnToFacetMapping.get(column));
        }
      }
      if (facets.isEmpty()) {
        return;
      }
      for (int partition : senseiCore.getPartitions()) {
        IndexReaderFactory<ZoieIndexReader<BoboIndexReader>> indexReaderFactory = senseiCore.getIndexReaderFactory(partition);
        if (indexReaderFactory == null) {
          continue;
        }
        List<ZoieIndexReader<BoboIndexReader>> indexReaders = null;
        try {
           indexReaders = indexReaderFactory.getIndexReaders();
           for (ZoieIndexReader<BoboIndexReader> zoieIndexReader : indexReaders) {            
             if (zoieIndexReader.getDocIDMaper().getDocID(uid) < 0) {
               continue;
             }
             if (zoieIndexReader instanceof ZoieMultiReader<?>) {
               for (ZoieIndexReader<BoboIndexReader> segmentReader : ((ZoieMultiReader<BoboIndexReader>) zoieIndexReader).getSequentialSubReaders()) {
                 if (!(segmentReader instanceof ZoieSegmentReader<?>)) {
                   throw new UnsupportedOperationException(segmentReader.getClass().toString());
                 }
                 updateExistingBoboIndexes((ZoieSegmentReader<BoboIndexReader>)segmentReader, uid, index, facets);
               }
             } else if (zoieIndexReader instanceof ZoieSegmentReader<?>) {
               updateExistingBoboIndexes((ZoieSegmentReader<BoboIndexReader>)zoieIndexReader, uid, index, facets);
             } else {
               throw new UnsupportedOperationException(zoieIndexReader.getClass().toString());
             }
           }
        } catch (IOException ex) {
          logger.error(ex.getMessage(), ex);
        } finally {
          if (indexReaders != null) {
            indexReaderFactory.returnIndexReaders(indexReaders);
          }
        }
      }
    }

    private void updateExistingBoboIndexes(ZoieSegmentReader<BoboIndexReader> segmentReader, long uid, int index, Set<String> facets) {
      int docId = segmentReader.getDocIDMaper().getDocID(uid);
      if (docId < 0) {
        return;
      }
      BoboIndexReader decoratedReader = segmentReader.getDecoratedReader();
      for (String facet : facets) {
        Object facetData = decoratedReader.getFacetData(facet);
        if (!(facetData instanceof int[])) {
          logger.warn("The facet " + facet + " should have a facet data of type int[] but not " + facetData.getClass().toString());
          continue;
        }
        int[] indexes = (int[]) facetData;
        if (indexes.length  <= docId) {
          logger.warn(String.format("The facet [%s] is supposed to contain the uid [%s] as the docid [%s], but its index array is only [%s] long", facet, uid, docId, indexes.length));
          facetMappingMismatch.inc();
          continue;
        }
        if (indexes[docId] > -1 && indexes[docId] != index) {
          logger.warn(String.format("The facet [%s] is supposed to contain the uid [%s] as the docid [%s], with docId index [%s] but it contains index [%s]", facet, uid, docId, index, indexes[docId]));
          facetMappingMismatch.inc();
          continue;
        }
        if (indexes[docId] == -1) {
          indexes[docId] = index;
          recoveredIndexInBoboFacetDataCache.inc();
        }
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
