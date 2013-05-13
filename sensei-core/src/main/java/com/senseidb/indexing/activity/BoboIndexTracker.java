package com.senseidb.indexing.activity;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;

import org.apache.log4j.Logger;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.RAMDirectory;

import proj.zoie.api.DocIDMapper;
import proj.zoie.api.IndexReaderFactory;
import proj.zoie.api.ZoieIndexReader;
import proj.zoie.api.ZoieSegmentReader;

import com.browseengine.bobo.api.BoboIndexReader;
import com.senseidb.search.node.SenseiCore;
import com.senseidb.search.node.SenseiIndexReaderDecorator.BoboListener;
import com.senseidb.search.plugin.PluggableSearchEngineManager;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.MetricName;

public class BoboIndexTracker implements BoboListener {
  private final static Logger logger = Logger.getLogger(PluggableSearchEngineManager.class);
  private Object dummyValue = new Object();
  private Map<BoboIndexReader, Object> readers = new WeakHashMap<BoboIndexReader, Object>();
  private static Counter recoveredIndexInBoboFacetDataCache;
  private static Counter facetMappingMismatch;
  private static Counter numberOfCachedReaders;
  private static Counter numberOfDeletedReaders;
  private static Counter numberOfCreatedReaders;

  private  SenseiCore senseiCore;
  static {
    recoveredIndexInBoboFacetDataCache = Metrics.newCounter(new MetricName(CompositeActivityManager.class,
        "recoveredIndexInBoboFacetDataCache"));
    facetMappingMismatch = Metrics.newCounter(new MetricName(BoboIndexTracker.class, "facetMappingMismatch"));
    numberOfCachedReaders = Metrics.newCounter(new MetricName(BoboIndexTracker.class, "numberOfCachedReaders"));
    numberOfDeletedReaders = Metrics.newCounter(new MetricName(BoboIndexTracker.class, "numberOfDeletedReaders"));
    numberOfCreatedReaders = Metrics.newCounter(new MetricName(BoboIndexTracker.class, "numberOfCreatedReaders"));
  }
 

  public synchronized void updateExistingBoboIndexes(long uid, int index, Set<String> facets) {
    boolean deletedSegments = false;
    for (int partition : senseiCore.getPartitions()) {
      IndexReaderFactory<ZoieIndexReader<BoboIndexReader>> indexReaderFactory = senseiCore.getIndexReaderFactory(partition);
      List<ZoieIndexReader<BoboIndexReader>> indexReaders = null;
      try {
        indexReaders = indexReaderFactory.getIndexReaders();
        List<BoboIndexReader> boboReaders = ZoieSegmentReader.extractDecoratedReaders(indexReaders);
        for (BoboIndexReader boboIndexReader : boboReaders) {
          ZoieSegmentReader<BoboIndexReader> zoieSegmentReader = (ZoieSegmentReader<BoboIndexReader>) boboIndexReader.getInnerReader();   
          if (!isSegmentOnDisk(zoieSegmentReader)) {
            continue;
          }
          if (readers.remove(boboIndexReader) != null) {
            numberOfDeletedReaders.inc();
            deletedSegments = true;
          }         
          recoverReaderIfNeeded(uid, index, facets, boboIndexReader);
        }
      } catch (IOException ex) {
        logger.error(ex.getMessage(), ex);
      } finally {
        if (indexReaders != null) {
          indexReaderFactory.returnIndexReaders(indexReaders);
        }
      }
     
    }
    if (deletedSegments) {
      numberOfCachedReaders.clear();
      numberOfCachedReaders.inc(readers.size());
    }
    for (BoboIndexReader boboSegmentReader : readers.keySet()) {
      if (boboSegmentReader != null) {
        recoverReaderIfNeeded(uid, index, facets, boboSegmentReader);
      }
    }
  }

  private final  void recoverReaderIfNeeded(long uid, int index, Set<String> facets, BoboIndexReader boboIndexReader) {
    ZoieSegmentReader<BoboIndexReader> zoieSegmentReader = (ZoieSegmentReader<BoboIndexReader>) boboIndexReader.getInnerReader();
    if (zoieSegmentReader == null) return;
    DocIDMapper mapper = zoieSegmentReader.getDocIDMaper();
    if (mapper == null) return;
    int docId = mapper.getDocID(uid);
    if (docId < 0) {
      return ;
    }
    BoboIndexReader decoratedReader = (BoboIndexReader) zoieSegmentReader.getDecoratedReader();
    for (String facet : facets) {
      Object facetData = decoratedReader.getFacetData(facet);
      if (!(facetData instanceof int[])) {
        logger.warn("The facet " + facet + " should have a facet data of type int[] but not " + facetData.getClass().toString());
        continue;
      }
      int[] indexes = (int[]) facetData;
      if (indexes.length <= docId) {
        logger.warn(String.format(
            "The facet [%s] is supposed to contain the uid [%s] as the docid [%s], but its index array is only [%s] long", facet, uid,
            docId, indexes.length));
        facetMappingMismatch.inc();
        continue;
      }
      if (indexes[docId] > -1 && indexes[docId] != index) {
        logger.warn(String.format(
            "The facet [%s] is supposed to contain the uid [%s] as the docid [%s], with docId index [%s] but it contains index [%s]",
            facet, uid, docId, index, indexes[docId]));
        facetMappingMismatch.inc();
        continue;
      }
      if (indexes[docId] == -1) {
        indexes[docId] = index;
        recoveredIndexInBoboFacetDataCache.inc();
      }
    }
    return;
  }

  protected boolean isSegmentOnDiskAndBig(ZoieSegmentReader zoieSegmentReader) {
    return isSegmentOnDisk(zoieSegmentReader) && zoieSegmentReader.getUIDArray().length > 2000;
  }

  protected boolean isSegmentOnDisk(ZoieSegmentReader zoieSegmentReader) {
    return zoieSegmentReader.directory() != null && !(zoieSegmentReader.directory() instanceof RAMDirectory);
  }

  @Override
  public void indexCreated(BoboIndexReader boboIndexReader) {
    ZoieSegmentReader<BoboIndexReader> zoieSegmentReader = (ZoieSegmentReader<BoboIndexReader>) boboIndexReader.getInnerReader();
    if (isSegmentOnDiskAndBig(zoieSegmentReader)) {
      synchronized (this) {
        if (readers.containsKey(boboIndexReader)) {
          return;
        }
        readers.put(boboIndexReader, dummyValue);
        numberOfCachedReaders.clear();
        numberOfCachedReaders.inc(readers.size());
        numberOfCreatedReaders.inc();
      }
    }
  }

  @Override
  public void indexDeleted(IndexReader boboIndexReader) {
  }
  public SenseiCore getSenseiCore() {
    return senseiCore;
  }
  public void setSenseiCore(SenseiCore senseiCore) {
    this.senseiCore = senseiCore;
  }
}
