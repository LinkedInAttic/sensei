package com.senseidb.indexing.activity;

import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.RAMDirectory;

import proj.zoie.api.ZoieSegmentReader;

import com.browseengine.bobo.api.BoboIndexReader;
import com.senseidb.search.node.SenseiIndexReaderDecorator.BoboListener;
import com.senseidb.search.plugin.PluggableSearchEngineManager;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.MetricName;

public class BoboIndexTracker implements BoboListener {
  private final static Logger logger = Logger.getLogger(PluggableSearchEngineManager.class);

  private Map<IndexReader, ZoieSegmentReader> readers = new HashMap<IndexReader, ZoieSegmentReader>();
  private static Counter recoveredIndexInBoboFacetDataCache;
  private static Counter facetMappingMismatch;
  private static Counter numberOfCachedReaders;
  private static Counter numberOfDeletedReaders;
  static {
    recoveredIndexInBoboFacetDataCache = Metrics.newCounter(new MetricName(CompositeActivityManager.class,
        "recoveredIndexInBoboFacetDataCache"));
    facetMappingMismatch = Metrics.newCounter(new MetricName(BoboIndexTracker.class, "facetMappingMismatch"));
    numberOfCachedReaders = Metrics.newCounter(new MetricName(BoboIndexTracker.class, "numberOfCachedReaders"));
    numberOfDeletedReaders = Metrics.newCounter(new MetricName(BoboIndexTracker.class, "numberOfDeletedReaders"));
  }


  public synchronized void updateExistingBoboIndexes(long uid, int index, Set<String> facets) {


    for (ZoieSegmentReader zoieSegmentReader : readers.values()) {
      int docId = zoieSegmentReader.getDocIDMaper().getDocID(uid);
      if (docId < 0) {
        continue;
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
    }
  }

  protected boolean isSegmentOnDisk(ZoieSegmentReader zoieSegmentReader) {
    return !(zoieSegmentReader.directory() instanceof RAMDirectory);
  }

  @Override
  public void indexCreated(BoboIndexReader boboIndexReader) {
    ZoieSegmentReader<BoboIndexReader> zoieSegmentReader = (ZoieSegmentReader<BoboIndexReader>) boboIndexReader.getInnerReader();
    if (isSegmentOnDisk(zoieSegmentReader)) {
      synchronized (this) {
        if (readers.containsKey(boboIndexReader)) {
          return;
        }
        readers.put(boboIndexReader, zoieSegmentReader);
        numberOfCachedReaders.clear();
        numberOfCachedReaders.inc(readers.size());
      }
    }
  }

  @Override
  public void indexDeleted(BoboIndexReader boboIndexReader) {
    ZoieSegmentReader<BoboIndexReader> zoieSegmentReader = (ZoieSegmentReader<BoboIndexReader>) boboIndexReader.getInnerReader();
    if (isSegmentOnDisk(zoieSegmentReader)) {
      synchronized (this) {
        readers.remove(boboIndexReader);
        numberOfDeletedReaders.inc();
        numberOfCachedReaders.clear();
        numberOfCachedReaders.inc(readers.size());
      }
    }
  }

}
