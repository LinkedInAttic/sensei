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

public class SynchronizedActivityRangeFacetHandler extends FacetHandler<int[]> {
  private static final String DEFAULT_FORMATTING_STRING = "0000000000";
  
  
  protected ThreadLocal<DecimalFormat> formatter = new ThreadLocal<DecimalFormat>() {
    protected DecimalFormat initialValue() {
      return new DecimalFormat(DEFAULT_FORMATTING_STRING);
    }
  };

  private final ActivityIntValues activityIntValues;
  private final CompositeActivityValues compositeActivityValues;
  

  
  public SynchronizedActivityRangeFacetHandler(String facetName, String fieldName, CompositeActivityValues compositeActivityValues, ActivityIntValues activityIntValues) {
    super(facetName, new HashSet<String>());
    this.compositeActivityValues = compositeActivityValues;
    this.activityIntValues = activityIntValues;   
  }
  @Override
  public int[] load(BoboIndexReader reader) throws IOException {
    ZoieSegmentReader<?> zoieReader = (ZoieSegmentReader<?>)(reader.getInnerReader());    
    long[] uidArray = zoieReader.getUIDArray();   
    return compositeActivityValues.precomputeArrayIndexes(uidArray);    
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
              return new ActivityRangeFilterSynchronizedIterator(array, indexes, startValue, endValue);           
          }
          
          @Override
          public boolean get(int docId) {           
            synchronized (activityIntValues.getFieldValues()) {
              int val = array[indexes[docId]];            
              return val >= startValue && val < endValue && val != Integer.MIN_VALUE;
            }
          }
        };
      }
    };
  }

  @Override
  public FacetCountCollectorSource getFacetCountCollectorSource(BrowseSelection sel, FacetSpec fspec) {
   throw new UnsupportedOperationException("Facets on activity columns are unsupported");
  }
  
  
  @Override
  public Object[] getRawFieldValues(BoboIndexReader reader, int id) {    
    final int[] indexes = (int[]) ((BoboIndexReader)reader).getFacetData(_name);   
    int index = indexes[id];
    if (index == -1) {
      return new String[0];
    }
    synchronized (activityIntValues.getFieldValues()) {
      return new Object[] {activityIntValues.getValue(index)};
    }  
  }
  @Override
  public String[] getFieldValues(BoboIndexReader reader, int id) {   
    final int[] indexes = (int[]) ((BoboIndexReader)reader).getFacetData(_name);   
    int index = indexes[id];   
    int value = 0;   
     synchronized (activityIntValues.getFieldValues()) {
       value = activityIntValues.getValue(index);
     }   
    if (value == Integer.MIN_VALUE) {
      return new String[0];
    }
    return new String[] {formatter.get().format(value)};
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
              synchronized (activityIntValues.getFieldValues()) {
                return array[indexes[doc.doc]]; 
              }
          }

          @Override
          public int compare(ScoreDoc doc1, ScoreDoc doc2) {  
            synchronized (activityIntValues.getFieldValues()) {
              int val1 = array[indexes[doc1.doc]]; 
              int val2 = array[indexes[doc2.doc]];            
              return (val1<val2 ? -1 : (val1==val2 ? 0 : 1));
            }
          }
        };
      }
    };
  }
  public static int[] parseRaw(String rangeString)
  {
    String[] ranges = FacetRangeFilter.getRangeStrings(rangeString);
      String lower=ranges[0];
      String upper=ranges[1];
      String includeLower = ranges[2];
      String includeUpper = ranges[3];
      int start = 0;
      int end = 0;
      if ("*".equals(lower))
      {
        start=Integer.MIN_VALUE;
      } else {
        start = Integer.parseInt(lower);
        if ("false".equals(includeLower)) {
          start++;
        }
      }
      if ("*".equals(upper))
      {
        end=Integer.MAX_VALUE;
      } else {
        end =  Integer.parseInt(upper);
        if ("true".equals(includeUpper)) {
          end++;
        }
      }     
      return new int[]{start,end};
  }
}
