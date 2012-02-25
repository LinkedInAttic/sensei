package com.senseidb.indexing.activity.facet;

import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.io.IOException;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.ScoreDoc;

import proj.zoie.api.DocIDMapper;
import proj.zoie.api.ZoieSegmentReader;

import com.browseengine.bobo.api.BoboIndexReader;
import com.browseengine.bobo.api.BrowseSelection;
import com.browseengine.bobo.api.FacetSpec;
import com.browseengine.bobo.facets.FacetCountCollectorSource;
import com.browseengine.bobo.facets.FacetHandler;
import com.browseengine.bobo.facets.filter.RandomAccessFilter;
import com.browseengine.bobo.sort.DocComparator;
import com.browseengine.bobo.sort.DocComparatorSource;
import com.senseidb.indexing.activity.ActivityFieldValues;

public class ActivitySimpleFacetHandler extends FacetHandler<int[]> {
  
  private final ActivityFieldValues activityFieldValues;
  private int[] fieldValues;

  public ActivitySimpleFacetHandler(String facetName, ActivityFieldValues activityFieldValues) {
    super(facetName, new HashSet<String>());
    // TODO Auto-generated constructor stub
    this.activityFieldValues = activityFieldValues;
    fieldValues = activityFieldValues.getFieldValues();
  }

  @Override
  public int[] load(BoboIndexReader reader) throws IOException {
    ZoieSegmentReader<?> zoieReader = (ZoieSegmentReader<?>)(reader.getInnerReader());
    
    long[] uidArray = zoieReader.getUIDArray();
    return activityFieldValues.precomputeArrayIndexes(uidArray);    
  }

  @Override
  public RandomAccessFilter buildRandomAccessFilter(String value, Properties selectionProperty) throws IOException {
    return null;
  }

  @Override
  public FacetCountCollectorSource getFacetCountCollectorSource(BrowseSelection sel, FacetSpec fspec) {
    return null;
  }

  @Override
  public String[] getFieldValues(BoboIndexReader reader, int id) {
    return null;
  }

  @Override
  public DocComparatorSource getDocComparatorSource() {
    return new DocComparatorSource() {
      @Override
      public DocComparator getComparator(IndexReader reader, int docbase) throws IOException {
        final int[] indexes = (int[]) ((BoboIndexReader)reader).getFacetData(_name);          
        return new DocComparator() {          
          @Override
          public Comparable<Integer> value(ScoreDoc doc) {            
            return fieldValues[indexes[doc.doc]];
          }          
          @Override
          public int compare(ScoreDoc doc1, ScoreDoc doc2) {            
            return fieldValues[indexes[doc1.doc]] - fieldValues[indexes[doc2.doc]];
          }
        };
      }};
  }

}
