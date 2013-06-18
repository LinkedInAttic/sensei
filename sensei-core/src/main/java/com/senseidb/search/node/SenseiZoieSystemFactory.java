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
package com.senseidb.search.node;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.senseidb.metrics.MetricFactory;
import com.senseidb.metrics.MetricName;
import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.lucene.search.Filter;

import proj.zoie.api.DefaultDirectoryManager;
import proj.zoie.api.DirectoryManager;
import proj.zoie.api.DirectoryManager.DIRECTORY_MODE;
import proj.zoie.api.indexing.IndexingEventListener;
import proj.zoie.api.indexing.OptimizeScheduler;
import proj.zoie.api.indexing.ZoieIndexableInterpreter;
import proj.zoie.impl.indexing.IndexUpdatedEvent;
import proj.zoie.impl.indexing.ZoieConfig;
import proj.zoie.impl.indexing.ZoieSystem;

import com.browseengine.bobo.api.BoboIndexReader;

public class SenseiZoieSystemFactory<T> extends SenseiZoieFactory<T>
{
  private static Logger log = Logger.getLogger(SenseiZoieSystemFactory.class);
  private Filter _purgeFilter = null;
  private OptimizeScheduler _optimizeScheduler;
  
  private Map<Integer,IndexingMetrics> metricsMap = new HashMap<Integer,IndexingMetrics>(); 
  
  public SenseiZoieSystemFactory(File idxDir,DIRECTORY_MODE dirMode, ZoieIndexableInterpreter<T> interpreter, SenseiIndexReaderDecorator indexReaderDecorator,
                                 ZoieConfig zoieConfig)
  {
    super(idxDir,dirMode,interpreter,indexReaderDecorator,zoieConfig);
  }
  
  public void setPurgeFilter(Filter purgeFilter){
    _purgeFilter = purgeFilter;
  }

  public void setOptimizeScheduler(OptimizeScheduler optimizeScheduler)
  {
    _optimizeScheduler = optimizeScheduler;
  }

  @Override
  public ZoieSystem<BoboIndexReader,T> getZoieInstance(int nodeId,final int partitionId)
  {
    File partDir = getPath(nodeId,partitionId);
    if(!partDir.exists())
    {
      partDir.mkdirs();
      log.info("nodeId="+nodeId+", partition=" + partitionId + " does not exist, directory created.");
    }
    
    DirectoryManager dirMgr = new DefaultDirectoryManager(partDir, _dirMode);
    ZoieSystem<BoboIndexReader,T> zoie = new ZoieSystem<BoboIndexReader,T>(dirMgr, _interpreter, _indexReaderDecorator, _zoieConfig);
    if (_purgeFilter!=null){
      zoie.setPurgeFilter(_purgeFilter);
    }
    if (_optimizeScheduler != null) {
      zoie.setOptimizeScheduler(_optimizeScheduler);
    }
    
    metricsMap.put(partitionId, new IndexingMetrics(partitionId));
    
    zoie.addIndexingEventListener(new IndexingEventListener() {
      
      @Override
      public void handleUpdatedDiskVersion(String updateDiskVersion) {
        // TODO Auto-generated method stub
        
      }
      
      @Override
      public void handleIndexingEvent(IndexingEvent evt) {
        if (evt instanceof IndexUpdatedEvent){
          IndexingMetrics metrics = SenseiZoieSystemFactory.this.metricsMap.get(partitionId);
          
          IndexUpdatedEvent updateEvent = (IndexUpdatedEvent)evt;
          
          metrics.docsIndexedMetric.mark(updateEvent.getNumDocsIndexed());
          metrics.docsLeftoverMetric.mark(updateEvent.getNumDocsLeftInQueue());
          
          metrics.flushTimeHistogram.update(updateEvent.getEndIndexingTime()-updateEvent.getStartIndexingTime());
          
        }
      }
    });
    return zoie;
  }
  
  // TODO: change to getDirectoryManager
  public File getPath(int nodeId,int partitionId)
  {
    return getPath(_idxDir,nodeId,partitionId);
  }
  
  private static class IndexingMetrics{
    final Meter docsIndexedMetric;
    final Meter docsLeftoverMetric;
    final Histogram flushTimeHistogram;
        
    IndexingMetrics(int partition){
      MetricName docsIndexedName =  new MetricName("docs-indexed","indexer");
      docsIndexedMetric = MetricFactory.newMeter(docsIndexedName);

      MetricName docsLeftoverName = new MetricName("docs-leftover","indexer");
      docsLeftoverMetric = MetricFactory.newMeter(docsLeftoverName);

      MetricName flushTimeName = new MetricName("flush-time","indexer");
      flushTimeHistogram = MetricFactory.newHistogram(flushTimeName);
    }
  }
}
