package com.senseidb.search.facet.attribute;

import java.io.IOException;

import org.apache.lucene.search.DocIdSetIterator;

import com.browseengine.bobo.api.BoboIndexReader;
import com.browseengine.bobo.docidset.EmptyDocIdSet;
import com.browseengine.bobo.docidset.RandomAccessDocIdSet;
import com.browseengine.bobo.facets.data.FacetDataCache;

import com.browseengine.bobo.facets.filter.RandomAccessFilter;
import com.browseengine.bobo.facets.range.SimpleDataCacheBuilder;

public class PredicateFacetFilter extends RandomAccessFilter {
  private final SimpleDataCacheBuilder dataCacheBuilder;
  private final FacetPredicate facetPredicate;
  
  public PredicateFacetFilter(SimpleDataCacheBuilder dataCacheBuilder, FacetPredicate facetPredicate) {
    this.dataCacheBuilder = dataCacheBuilder;
    this.facetPredicate = facetPredicate;
  }
  
  
  @Override
  public RandomAccessDocIdSet getRandomAccessDocIdSet(BoboIndexReader reader) throws IOException {
    final FacetDataCache facetDataCache = dataCacheBuilder.build(reader);    
    int startDocIdTemp = Integer.MAX_VALUE;
    int endDocIdTemp = -1;
    for (int i = facetPredicate.valueStartIndex(facetDataCache); i < facetPredicate.valueEndIndex(facetDataCache); i++) {
      if (facetPredicate.evaluateValue(facetDataCache, i)) {
        if (!facetPredicate.evaluateValue(facetDataCache, i)) {
          continue;
        }
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
    FacetDataCache dataCache = dataCacheBuilder.build(reader);
    int[] frequencies = dataCache.freqs;
    double selectivity = 0;
    int accumFreq = 0;
    int total = reader.maxDoc();  
    for (int i = facetPredicate.valueStartIndex(dataCache); i < facetPredicate.valueEndIndex(dataCache); i++) {
      if (!facetPredicate.evaluateValue(dataCache, i)) {
        continue;
      }
      accumFreq += frequencies[i];      
    }
    selectivity = (double) accumFreq / (double) total;
    if (selectivity > 0.999) {
      selectivity = 1.0;
    }
    return selectivity;
  }

}
