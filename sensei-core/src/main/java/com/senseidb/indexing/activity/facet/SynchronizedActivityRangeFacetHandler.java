package com.senseidb.indexing.activity.facet;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashSet;
import java.util.Properties;

import org.apache.lucene.index.IndexReader;
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
import com.senseidb.indexing.activity.ActivityIntValues;
import com.senseidb.indexing.activity.CompositeActivityValues;

/**
 * Used only for testing
 * @author vzhabiuk
 *
 */
public class SynchronizedActivityRangeFacetHandler extends ActivityRangeFacetHandler {
  public static final Object GLOBAL_ACTIVITY_TEST_LOCK = new Object();

  private final ActivityIntValues activityIntValues;
  private final CompositeActivityValues compositeActivityValues;
  
  public SynchronizedActivityRangeFacetHandler(String facetName, String fieldName, CompositeActivityValues compositeActivityValues, ActivityIntValues activityIntValues) {
    super(facetName, fieldName, compositeActivityValues, activityIntValues);
    this.compositeActivityValues = compositeActivityValues;
    this.activityIntValues = activityIntValues;   
  }

  @Override
  public RandomAccessFilter buildRandomAccessFilter(final String value, Properties selectionProperty) throws IOException {
    return new RandomAccessFilter() {      
      @Override
      public RandomAccessDocIdSet getRandomAccessDocIdSet(BoboIndexReader reader)
          throws IOException {
        final int[] indexes = (int[]) ((BoboIndexReader)reader).getFacetData(_name);
        if (value == null || value.isEmpty()) {
          return  EmptyDocIdSet.getInstance();
        }
        int[] range = parseRaw(value);
        final int startValue = range[0];
        final int endValue = range[1];
        if (startValue >= endValue) {
          return  EmptyDocIdSet.getInstance();
        }
        final int[] array = activityIntValues.getFieldValues();
        return new RandomAccessDocIdSet() {          
          @Override
          public DocIdSetIterator iterator() throws IOException {
             System.out.println(activityIntValues.getFieldName());
            return new ActivityRangeFilterSynchronizedIterator(array, indexes, startValue, endValue);           
          }
          
          @Override
          public boolean get(int docId) {           
            if (indexes[docId] == -1) {
              return false;
            }
            synchronized (GLOBAL_ACTIVITY_TEST_LOCK) {
              int val = array[indexes[docId]];
              return val >= startValue && val < endValue && val != Integer.MIN_VALUE;
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
    final int[] array = activityIntValues.fieldValues;
    return new DocComparatorSource() {
      @Override
      public DocComparator getComparator(IndexReader reader, int docbase)
          throws IOException {
        final int[] indexes = (int[]) ((BoboIndexReader) reader)
            .getFacetData(_name);
        return new DocComparator() {
          @Override
          public Comparable<Integer> value(ScoreDoc doc) {          
              synchronized (GLOBAL_ACTIVITY_TEST_LOCK) {
                if (indexes[doc.doc] == -1) {
                  return 0;
                }
                return array[indexes[doc.doc]]; 
              }
          }

          @Override
          public int compare(ScoreDoc doc1, ScoreDoc doc2) {  
            
            synchronized (GLOBAL_ACTIVITY_TEST_LOCK) {
              int val1 = indexes[doc1.doc] > -1 ? array[indexes[doc1.doc]] : 0; ; 
              int val2 = indexes[doc2.doc] > -1 ?array[indexes[doc2.doc]] : 0;            
              return (val1<val2 ? -1 : (val1==val2 ? 0 : 1));
            }
          }
        };
      }
    };
  }
  
}
