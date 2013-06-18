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
package com.senseidb.indexing.activity;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Timer;
import com.senseidb.metrics.MetricFactory;
import com.senseidb.metrics.MetricName;
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

public class PurgeUnusedActivitiesJob implements Runnable, PurgeUnusedActivitiesJobMBean {
  private final static Logger logger = Logger.getLogger(PurgeUnusedActivitiesJob.class);
  
  private final CompositeActivityValues compositeActivityValues;
  private final Set<IndexReaderFactory<ZoieIndexReader<BoboIndexReader>>> zoieSystems;
  private static Timer timer = MetricFactory.newTimer(new MetricName(PurgeUnusedActivitiesJob.class, "purgeUnusedActivityIndexes"));
  private static Counter foundActivitiesToPurge = MetricFactory.newCounter(new MetricName(PurgeUnusedActivitiesJob.class, "foundActivitiesToPurge"));
  private static Counter recentUidsSavedFromPurge = MetricFactory.newCounter(new MetricName(PurgeUnusedActivitiesJob.class, "recentUidsSavedFromPurge"));
  
  protected ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();

  private final long frequencyInMillis;
  public PurgeUnusedActivitiesJob(CompositeActivityValues compositeActivityValues, Set<IndexReaderFactory<ZoieIndexReader<BoboIndexReader>>> zoieSystems, long frequencyInMillis) {
    this.compositeActivityValues = compositeActivityValues;
    this.zoieSystems = zoieSystems;
    this.frequencyInMillis = frequencyInMillis;
    
  }
  public void start() {
    if (frequencyInMillis > 0) {
      executorService.scheduleAtFixedRate(this, frequencyInMillis, frequencyInMillis, TimeUnit.MILLISECONDS); 
    }
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
    int bitSetLength = keys.length;
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
    int recovered = compositeActivityValues.recentlyAddedUids.markRecentAsFoundInBitSet(keys, foundSet, bitSetLength);
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
