/**
 * This software is licensed to you under the Apache License, Version 2.0 (the
 * "Apache License").
 *
 * LinkedIn's contributions are made under the Apache License. If you contribute
 * to the Software, the contributions will be deemed to have been made under the
 * Apache License, unless you expressly indicate otherwise. Please do not make any
 * contributions that would be inconsistent with the Apache License.
 *
 * You may obtain a copy of the Apache License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, this software
 * distributed under the Apache License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the Apache
 * License for the specific language governing permissions and limitations for the
 * software governed under the Apache License.
 *
 * © 2012 LinkedIn Corp. All Rights Reserved.  
 */
package com.senseidb.search.plugin;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.lucene.index.IndexReader;
import org.json.JSONException;
import org.json.JSONObject;

import proj.zoie.api.Zoie;
import proj.zoie.api.ZoieIndexReader;
import proj.zoie.api.ZoieMultiReader;
import proj.zoie.api.ZoieSegmentReader;
import proj.zoie.hourglass.impl.HourglassListener;

import com.browseengine.bobo.facets.FacetHandler;
import com.senseidb.conf.SenseiSchema;
import com.senseidb.indexing.ShardingStrategy;
import com.senseidb.indexing.activity.CompositeActivityManager;
import com.senseidb.indexing.activity.deletion.DeletionListener;
import com.senseidb.plugin.SenseiPluginRegistry;
import com.senseidb.search.node.SenseiCore;

public class PluggableSearchEngineManager implements DeletionListener, HourglassListener<IndexReader, IndexReader> { 
  private final static Logger logger = Logger.getLogger(PluggableSearchEngineManager.class);
  private ShardingStrategy shardingStrategy;
  private SenseiCore senseiCore; 
  private String version;
  private Comparator<String> versionComparator;
  private SenseiSchema senseiSchema;
  private SenseiPluginRegistry pluginRegistry;
  private int nodeId;
  private List<PluggableSearchEngine> pluggableEngines = new ArrayList<PluggableSearchEngine>();
  private int maxPartition;
  private boolean acceptEventsForAllPartitions;
 
  public PluggableSearchEngineManager() {
    
  }
  public String getOldestVersion() {

     return version;     
  }
 
  public boolean acceptEventsForAllPartitions() {
    return acceptEventsForAllPartitions;
  }
  public final void init(String indexDirectory, int nodeId, SenseiSchema senseiSchema, Comparator<String> versionComparator, SenseiPluginRegistry pluginRegistry, ShardingStrategy shardingStrategy) {
    this.nodeId = nodeId;
    this.senseiSchema = senseiSchema;
    this.versionComparator = versionComparator;

    this.pluginRegistry = pluginRegistry;
    this.shardingStrategy = shardingStrategy;    


    maxPartition = pluginRegistry.getConfiguration().getInt("sensei.index.manager.default.maxpartition.id") + 1;
    pluggableEngines = new ArrayList<PluggableSearchEngine>(pluginRegistry.resolveBeansByListKey("sensei.search.pluggableEngines", PluggableSearchEngine.class));
    if (CompositeActivityManager.activitiesPresent(senseiSchema)) {
      pluggableEngines.add(new CompositeActivityManager());
    }
    
    
    acceptEventsForAllPartitions = false;
    for (PluggableSearchEngine engine : pluggableEngines) {
      engine.init(indexDirectory, nodeId, senseiSchema, versionComparator, pluginRegistry, shardingStrategy);
      if (engine.acceptEventsForAllPartitions()) {
        acceptEventsForAllPartitions = true;
      }
    }

    initVersion(versionComparator);
    
  }
  public void initVersion(Comparator<String> versionComparator) {
    List<String> versions = new ArrayList<String>();
    for (PluggableSearchEngine engine : pluggableEngines) {
      if (engine.getVersion() != null && !"".equals(engine.getVersion())) {
        versions.add(engine.getVersion());
      }
    }
    if (versions.size() > 0) {
      String version = versions.get(0);
      for (String ver : versions) {
        if (versionComparator.compare(version, ver) > 0) {
          version = ver;
        }
      }
    }
  }
  
 
  /**
   * Updates all the corresponding activity columns found in the document
   * @param event
   * @param version
   * @return 
   */
  public JSONObject update(JSONObject event, String version) {
    if (this.version != null && versionComparator.compare(this.version, version) > 0) {
      return event;
    } else {
      this.version = version;
    }
    boolean validForCurrentNode;
    try {
      validForCurrentNode = Arrays.binarySearch(senseiCore.getPartitions(), shardingStrategy.caculateShard(maxPartition, event)) >= 0;
    } catch (JSONException e) {
      throw new RuntimeException(e);
    }
    for (PluggableSearchEngine pluggableSearchEngine : pluggableEngines) {
      if (pluggableSearchEngine.acceptEventsForAllPartitions() || validForCurrentNode) {
        try {
          event = pluggableSearchEngine.acceptEvent(event, version);
        } catch (Exception ex) {
          logger.error(ex.getMessage(), ex);
        }
      }
    }
    return event;
  }
  
  
  public void close() {
    for (PluggableSearchEngine pluggableSearchEngine : pluggableEngines) {
      pluggableSearchEngine.stop();
    } 
  }
 



  /* (non-Javadoc)
   * @see com.senseidb.indexing.activity.deletion.DeletionListener#onDelete(org.apache.lucene.index.IndexReader, long[])
   */
  @Override
  public void onDelete(IndexReader indexReader, long... uids) {
    for (PluggableSearchEngine pluggableSearchEngine : pluggableEngines) {
      pluggableSearchEngine.onDelete(indexReader, uids);
    } 
  }

  @Override
  public void onNewZoie(Zoie<IndexReader, IndexReader> zoie) {    
    
  }
  @Override
  public void onRetiredZoie(Zoie<IndexReader, IndexReader> zoie) {    
    
  }
  @Override
  public void onIndexReaderCleanUp(ZoieIndexReader<IndexReader> indexReader) {
    if (indexReader instanceof ZoieMultiReader) {
      ZoieSegmentReader[] segments = (ZoieSegmentReader[]) ((ZoieMultiReader) indexReader).getSequentialSubReaders();
      for (ZoieSegmentReader segmentReader : segments) {
        handleSegment(segmentReader);
      }
    } else if (indexReader instanceof ZoieSegmentReader) {
      handleSegment((ZoieSegmentReader) indexReader);
    } else {
      throw new UnsupportedOperationException("Only segment and multisegment readers can be handled");
    }
    
  }
  private void handleSegment(ZoieSegmentReader segmentReader) {    
    onDelete(segmentReader, segmentReader.getUIDArray());      
  }
  
  public void start(SenseiCore senseiCore) {
    this.senseiCore = senseiCore;
    for (PluggableSearchEngine pluggableSearchEngine : pluggableEngines) {
      pluggableSearchEngine.start(senseiCore);
    }
  }
  
  public Set<String> getFieldNames() {
    Set<String> ret = new HashSet<String>();
    for (PluggableSearchEngine pluggableSearchEngine : pluggableEngines) {
      ret.addAll(pluggableSearchEngine.getFieldNames());
    }
    return ret;
  }
  

  public List<FacetHandler<?>> createFacetHandlers() {
    List<FacetHandler<?>> ret = new ArrayList<FacetHandler<?>>();
    for (PluggableSearchEngine pluggableSearchEngine : pluggableEngines) {
      ret.addAll(pluggableSearchEngine.createFacetHandlers());
    }
    return ret;
  }
  public Set<String> getFacetNames() {
    Set<String> ret = new HashSet<String>();
    for (PluggableSearchEngine pluggableSearchEngine : pluggableEngines) {
      ret.addAll(pluggableSearchEngine.getFacetNames());
    }
    return ret;
  }

  public List<PluggableSearchEngine> getPluggableEngines() {
    return pluggableEngines;
  }
   
}


