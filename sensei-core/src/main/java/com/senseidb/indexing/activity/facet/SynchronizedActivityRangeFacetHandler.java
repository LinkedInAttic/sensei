package com.senseidb.indexing.activity.facet;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashSet;
import java.util.Properties;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.ScoreDoc;

import proj.zoie.api.ZoieSegmentReader;

import com.browseengine.bobo.api.BoboIndexReader;
import com.browseengine.bobo.api.BrowseSelection;
import com.browseengine.bobo.api.FacetSpec;
import com.browseengine.bobo.docidset.EmptyDocIdSet;
import com.browseengine.bobo.docidset.RandomAccessDocIdSet;
import com.browseengine.bobo.facets.FacetCountCollectorSource;
import com.browseengine.bobo.facets.FacetHandler;
import com.browseengine.bobo.facets.filter.FacetRangeFilter;
import com.browseengine.bobo.facets.filter.RandomAccessFilter;
import com.browseengine.bobo.sort.DocComparator;
import com.browseengine.bobo.sort.DocComparatorSource;
import com.senseidb.indexing.activity.CompositeActivityValues;
import com.senseidb.indexing.activity.primitives.ActivityIntValues;
import com.senseidb.indexing.activity.primitives.ActivityPrimitiveValues;

/**
 * Used only for testing
 * 
 * @author vzhabiuk
 * 
 */
public class SynchronizedActivityRangeFacetHandler extends ActivityRangeFacetHandler {
  public static final Object GLOBAL_ACTIVITY_TEST_LOCK = new Object();

  public SynchronizedActivityRangeFacetHandler(String facetName, String fieldName, CompositeActivityValues compositeActivityValues,
      ActivityPrimitiveValues activityPrimitiveValues) {
    super(facetName, fieldName, compositeActivityValues, activityPrimitiveValues);
  }

  @Override
  public RandomAccessFilter buildRandomAccessFilter(final String value, final Properties selectionProperty) throws IOException {
    return new RandomAccessFilter() {
      @Override
      public RandomAccessDocIdSet getRandomAccessDocIdSet(final BoboIndexReader reader) throws IOException {
        final RandomAccessDocIdSet docIdSet = (RandomAccessDocIdSet) SynchronizedActivityRangeFacetHandler.super.buildRandomAccessFilter(
            value, selectionProperty).getDocIdSet(reader);
        return new RandomAccessDocIdSet() {
          @Override
          public DocIdSetIterator iterator() throws IOException {
            return new SynchronizedIterator(docIdSet.iterator());
          }

          @Override
          public boolean get(int docId) {
            synchronized (GLOBAL_ACTIVITY_TEST_LOCK) {
              return docIdSet.get(docId);
            }
          }
        };
      }
    };
  }

  @Override
  public Object[] getRawFieldValues(BoboIndexReader reader, int id) {
    synchronized (GLOBAL_ACTIVITY_TEST_LOCK) {
      return super.getRawFieldValues(reader, id);
    }
  }

  @Override
  public String[] getFieldValues(BoboIndexReader reader, int id) {
    synchronized (GLOBAL_ACTIVITY_TEST_LOCK) {
      return super.getFieldValues(reader, id);
    }
  }

  @Override
  public DocComparatorSource getDocComparatorSource() {
    DocComparatorSource docComparatorSource = SynchronizedActivityRangeFacetHandler.super.getDocComparatorSource();
    return new DocComparatorSource() {
      @Override
      public DocComparator getComparator(IndexReader reader, int docbase) throws IOException {
        final DocComparator comparator = SynchronizedActivityRangeFacetHandler.super.getDocComparatorSource()
            .getComparator(reader, docbase);
        return new DocComparator() {
          @Override
          public Comparable<Integer> value(ScoreDoc doc) {
            synchronized (GLOBAL_ACTIVITY_TEST_LOCK) {
              return comparator.value(doc);
            }
          }
          @Override
          public int compare(ScoreDoc doc1, ScoreDoc doc2) {
            synchronized (GLOBAL_ACTIVITY_TEST_LOCK) {
              return comparator.compare(doc1, doc2);
            }
          }
        };
      }
    };
  }
}
