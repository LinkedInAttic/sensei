package com.sensei.facet.attribute;

import java.io.IOException;

import org.apache.lucene.search.DocIdSetIterator;

import com.browseengine.bobo.api.BoboIndexReader;
import com.browseengine.bobo.docidset.EmptyDocIdSet;
import com.browseengine.bobo.docidset.RandomAccessDocIdSet;
import com.browseengine.bobo.facets.data.FacetDataCache;
import com.browseengine.bobo.facets.filter.AdaptiveFacetFilter.FacetDataCacheBuilder;
import com.browseengine.bobo.facets.filter.RandomAccessFilter;

public class PredicateFacetFilter extends RandomAccessFilter {
  private final FacetDataCacheBuilder dataCacheBuilder;
  private final FacetPredicate facetPredicate;
  
  public PredicateFacetFilter(FacetDataCacheBuilder dataCacheBuilder, FacetPredicate facetPredicate) {
    this.dataCacheBuilder = dataCacheBuilder;
    this.facetPredicate = facetPredicate;
  }
  
  
  @Override
  public RandomAccessDocIdSet getRandomAccessDocIdSet(BoboIndexReader reader) throws IOException {
    final FacetDataCache facetDataCache = dataCacheBuilder.build(reader);
    facetPredicate.init(facetDataCache);
    int startDocIdTemp = Integer.MAX_VALUE;
    int endDocIdTemp = -1;
    for (int i = facetPredicate.valueStartIndex(); i < facetPredicate.valueEndIndex(); i++) {
      if (facetPredicate.evaluateValue(facetDataCache, i)) {
        if (startDocIdTemp > facetDataCache.minIDs[i]) {
          startDocIdTemp = facetDataCache.minIDs[i];
        }
        if (endDocIdTemp < facetDataCache.maxIDs[i]) {
          endDocIdTemp = facetDataCache.maxIDs[i];
        }
      }
    }
    final int startDocId = startDocIdTemp;
    final int endDocId = endDocIdTemp;
    if (startDocId > endDocId) {
      return EmptyDocIdSet.getInstance();
    } 
    return new RandomAccessDocIdSet() {
      @Override
      public boolean get(int docId) {        
        return facetPredicate.evaluate(facetDataCache, docId);
      }
      @Override
      public DocIdSetIterator iterator() throws IOException {        
        
        return new PredicateDocIdIterator(startDocId, endDocId, facetPredicate, facetDataCache);
      }
      
    };
  }  
  @Override
  public double getFacetSelectivity(BoboIndexReader reader) {  
    int[] frequencies = dataCacheBuilder.build(reader).freqs;
    double selectivity = 0;
    int accumFreq = 0;
    int total = reader.maxDoc();  
    for (int i = facetPredicate.valueStartIndex(); i < facetPredicate.valueEndIndex(); i++) {
      accumFreq += frequencies[i];      
    }
    selectivity = (double) accumFreq / (double) total;
    if (selectivity > 0.999) {
      selectivity = 1.0;
    }
    return selectivity;
  }

}
