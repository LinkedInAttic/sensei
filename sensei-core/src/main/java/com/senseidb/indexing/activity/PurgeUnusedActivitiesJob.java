package com.senseidb.indexing.activity;

import it.unimi.dsi.fastutil.longs.LongIterator;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.BitSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.management.MBeanServer;
import javax.management.ObjectInstance;
import javax.management.ObjectName;

import org.apache.log4j.Logger;

import proj.zoie.api.DocIDMapper;
import proj.zoie.api.IndexReaderFactory;
import proj.zoie.api.ZoieIndexReader;

import com.browseengine.bobo.api.BoboIndexReader;
import com.senseidb.conf.SenseiConfParams;
import com.senseidb.plugin.SenseiPluginRegistry;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.Timer;

public class PurgeUnusedActivitiesJob implements Runnable, PurgeUnusedActivitiesJobMBean {
  private final static Logger logger = Logger.getLogger(PurgeUnusedActivitiesJob.class);
  
  private final CompositeActivityValues compositeActivityValues;
  private final Set<IndexReaderFactory<ZoieIndexReader<BoboIndexReader>>> zoieSystems;
  private static Timer timer = Metrics.newTimer(new MetricName(PurgeUnusedActivitiesJob.class, "purgeUnusedActivityIndexes"), TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
  private static Counter foundActivitiesToPurge = Metrics.newCounter(new MetricName(PurgeUnusedActivitiesJob.class, "foundActivitiesToPurge"));
  private static Counter recentUidsSavedFromPurge = Metrics.newCounter(new MetricName(PurgeUnusedActivitiesJob.class, "recentUidsSavedFromPurge"));
  
  protected ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();

  private final long frequencyInMillis;
  public PurgeUnusedActivitiesJob(CompositeActivityValues compositeActivityValues, Set<IndexReaderFactory<ZoieIndexReader<BoboIndexReader>>> zoieSystems, long frequencyInMillis) {
    this.compositeActivityValues = compositeActivityValues;
    this.zoieSystems = zoieSystems;
    this.frequencyInMillis = frequencyInMillis;
    
  }
  public void start() {
    executorService.scheduleAtFixedRate(this, frequencyInMillis, frequencyInMillis, TimeUnit.MILLISECONDS); 
    MBeanServer platformMBeanServer = ManagementFactory.getPlatformMBeanServer();
    ObjectName name;
    try {
      name = new ObjectName("com.senseidb.indexing.activity:type=PurgeUnusedActivitiesJobInvoke");
     Set<ObjectInstance> mbeans = platformMBeanServer.queryMBeans(name, null);
    if (mbeans != null && mbeans.isEmpty()) {
       platformMBeanServer.registerMBean(this, name);
     }        
    } catch (Exception e) {
      logger.error("Couldn't register the  PurgeUnusedActivitiesJob operation", e);
    }
  }
  public void stop() {
    executorService.shutdown();    
  }
  public void run()  {    
    try {
      timer.time(new Callable<Integer>() {
        @Override
        public Integer call() throws Exception {        
          return purgeUnusedActivityIndexes();
        }       
      });
    } catch (Exception e) {
      logger.error(e.getMessage(), e);
    }
  }
  public int purgeUnusedActivityIndexes() {
    logger.info("Starting the purgeUnusedActivitiesJob");
    long[] keys;
    try {
      compositeActivityValues.globalLock.readLock().lock();
      keys = new long[compositeActivityValues.uidToArrayIndex.size()];
      LongIterator iterator = compositeActivityValues.uidToArrayIndex.keySet().iterator();
      int i = 0;
      while (iterator.hasNext()) {
        keys[i++] = iterator.nextLong();
      }
    }  finally {
        compositeActivityValues.globalLock.readLock().unlock();
    }     
    BitSet foundSet = new BitSet(keys.length); 
    for (IndexReaderFactory<ZoieIndexReader<BoboIndexReader>> zoie : zoieSystems) {
      List<ZoieIndexReader<BoboIndexReader>> indexReaders = null;      
      try {
        indexReaders = zoie.getIndexReaders();
        for (int i = 0; i < keys.length; i++) {        
          if (foundSet.get(i)) {
            continue;
          }
          for (ZoieIndexReader<BoboIndexReader> zoieIndexReader : indexReaders) {           
            if (DocIDMapper.NOT_FOUND != zoieIndexReader.getDocIDMaper().getDocID(keys[i])) {
              foundSet.set(i);
              break;
            }
          }          
        }
      } catch (IOException e) {
        logger.error(e.getMessage(), e);
      } finally {
        if (indexReaders != null) {
          zoie.returnIndexReaders(indexReaders);
        }        
      }
    }
    int recovered = compositeActivityValues.recentlyAddedUids.markRecentAsFoundInBitSet(keys, foundSet);
    recentUidsSavedFromPurge.inc(recovered);
    int found = foundSet.cardinality();
    if (found == keys.length) {
      logger.info("purgeUnusedActivitiesJob found  no activities to purge");
      return 0;
    }
    long[] notFound = new long[keys.length - found];   
    int j = 0;
    for (int i = 0; i < keys.length; i++) {
      if (!foundSet.get(i)) {
        notFound[j] = keys[i];
        j++;
      }
    }
    compositeActivityValues.delete(notFound);
    logger.info("purgeUnusedActivitiesJob found  " + notFound.length + " activities to purge");
    foundActivitiesToPurge.inc(notFound.length);
    return notFound.length;
  }
  public static long extractFrequency(SenseiPluginRegistry pluginRegistry) {
    int minutes = pluginRegistry.getConfiguration().getInt(SenseiConfParams.SENSEI_INDEX_ACTIVITY_PURGE_FREQUENCY_MINUTES, 0);
    if (minutes != 0) {
      return 1000L*60 * minutes;
    }
    int hours = pluginRegistry.getConfiguration().getInt(SenseiConfParams.SENSEI_INDEX_ACTIVITY_PURGE_FREQUENCY_HOURS, 6);
    return 1000L*60 * 60 * hours;
  }
}
