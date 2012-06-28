package com.senseidb.search.plugin;

import java.util.Comparator;
import java.util.List;
import java.util.Set;

import org.apache.lucene.index.IndexReader;
import org.json.JSONObject;

import com.browseengine.bobo.facets.FacetHandler;
import com.senseidb.conf.SenseiSchema;
import com.senseidb.indexing.ShardingStrategy;
import com.senseidb.plugin.SenseiPluginRegistry;
import com.senseidb.search.node.SenseiCore;

public interface PluggableSearchEngine {
  public void init(String indexDirectory, int nodeId, SenseiSchema senseiSchema, Comparator<String> versionComparator, SenseiPluginRegistry pluginRegistry, ShardingStrategy shardingStrategy);
  public String getVersion();  
  public JSONObject acceptEvent(JSONObject event, String version);
  public boolean acceptEventsForAllPartitions();
  public Set<String> getFieldNames();
  public Set<String> getFacetNames();
  public List<FacetHandler<?>> createFacetHandlers();
  public void onDelete(IndexReader indexReader, long... uids);
  public void start(SenseiCore senseiCore);
  public void stop();  
}
