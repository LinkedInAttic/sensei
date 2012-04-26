package com.senseidb.search.node;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.apache.lucene.search.Filter;

import com.linkedin.zoie.api.DefaultDirectoryManager;
import com.linkedin.zoie.api.DirectoryManager;
import com.linkedin.zoie.api.DirectoryManager.DIRECTORY_MODE;
import com.linkedin.zoie.api.indexing.IndexingEventListener;
import com.linkedin.zoie.api.indexing.ZoieIndexableInterpreter;
import com.linkedin.zoie.impl.indexing.IndexUpdatedEvent;
import com.linkedin.zoie.impl.indexing.ZoieConfig;
import com.linkedin.zoie.impl.indexing.ZoieSystem;

import com.linkedin.bobo.api.BoboIndexReader;
import com.senseidb.metrics.MetricsConstants;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricName;

public class SenseiZoieSystemFactory<T> extends SenseiZoieFactory<T>
{
  private static Logger log = Logger.getLogger(SenseiZoieSystemFactory.class);
  private Filter _purgeFilter = null;
  
  private Map<Integer,IndexingMetrics> metricsMap = new HashMap<Integer,IndexingMetrics>(); 
  
  public SenseiZoieSystemFactory(File idxDir,DIRECTORY_MODE dirMode, ZoieIndexableInterpreter<T> interpreter, SenseiIndexReaderDecorator indexReaderDecorator,
                                 ZoieConfig zoieConfig)
  {
    super(idxDir,dirMode,interpreter,indexReaderDecorator,zoieConfig);
  }
  
  public void setPurgeFilter(Filter purgeFilter){
    _purgeFilter = purgeFilter;
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
      MetricName docsIndexedName =  new MetricName(MetricsConstants.Domain,"meter","docs-indexed","indexer");
      docsIndexedMetric = Metrics.newMeter(docsIndexedName, "indexing", TimeUnit.SECONDS);

      MetricName docsLeftoverName = new MetricName(MetricsConstants.Domain,"meter","docs-leftover","indexer");
      docsLeftoverMetric = Metrics.newMeter(docsLeftoverName, "indexing", TimeUnit.SECONDS);

      MetricName flushTimeName = new MetricName(MetricsConstants.Domain,"histogram","flush-time","indexer");
      flushTimeHistogram = Metrics.newHistogram(flushTimeName, false);
    }
  }
}
