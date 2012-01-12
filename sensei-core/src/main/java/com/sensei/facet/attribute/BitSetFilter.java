package com.sensei.facet.attribute;

import java.io.IOException;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.OpenBitSet;

import com.browseengine.bobo.api.BoboIndexReader;
import com.browseengine.bobo.docidset.EmptyDocIdSet;
import com.browseengine.bobo.docidset.RandomAccessDocIdSet;
import com.browseengine.bobo.facets.data.FacetDataCache;
import com.browseengine.bobo.facets.data.MultiValueFacetDataCache;
import com.browseengine.bobo.facets.filter.AdaptiveFacetFilter.FacetDataCacheBuilder;
import com.browseengine.bobo.facets.filter.FacetOrFilter;
import com.browseengine.bobo.facets.filter.MultiValueORFacetFilter;
import com.browseengine.bobo.facets.filter.RandomAccessFilter;

public class BitSetFilter extends RandomAccessFilter {  
    private static final long serialVersionUID = 1L;
    
    protected final FacetDataCacheBuilder facetDataCacheBuilder;
    protected final BitSetBuilder bitSetBuilder;
    private volatile OpenBitSet bitSet;
    private volatile FacetDataCache lastCache;
    public BitSetFilter(BitSetBuilder bitSetBuilder, FacetDataCacheBuilder facetDataCacheBuilder) {
      this.bitSetBuilder = bitSetBuilder;    
      this.facetDataCacheBuilder = facetDataCacheBuilder;      
    }
    public OpenBitSet getBitSet( FacetDataCache dataCache) {
      
      if (lastCache == dataCache) {
        return bitSet;
      }     
      bitSet = bitSetBuilder.bitSet(dataCache);
      lastCache = dataCache;
      return bitSet;
    }
    
    @Override
    public RandomAccessDocIdSet getRandomAccessDocIdSet(final BoboIndexReader reader) throws IOException {
      final FacetDataCache dataCache = facetDataCacheBuilder.build(reader);
      final OpenBitSet openBitSet = getBitSet(dataCache);
      long count = openBitSet.cardinality();
      if (count == 0) {
        return EmptyDocIdSet.getInstance();
      } else {
        final boolean multi = dataCache instanceof MultiValueFacetDataCache;
        final MultiValueFacetDataCache multiCache = multi ? (MultiValueFacetDataCache) dataCache : null;
        
        return new RandomAccessDocIdSet() {        
          public DocIdSetIterator iterator() {          
              
              if (multi) {
                return new MultiValueORFacetFilter.MultiValueOrFacetDocIdSetIterator(multiCache, openBitSet);
              } else {
                return new FacetOrFilter.FacetOrDocIdSetIterator(dataCache, openBitSet);  
                  
            }
          }
          public boolean get(int docId) {
            if (multi) {
              return multiCache._nestedArray.contains(docId, openBitSet);
            } else {
              return openBitSet.fastGet(dataCache.orderArray.get(docId));
            }
          }
        };
      }
    }

    @Override
    public double getFacetSelectivity(BoboIndexReader reader) {
      FacetDataCache dataCache = facetDataCacheBuilder.build(reader);
      final OpenBitSet openBitSet = getBitSet(dataCache);
      int[] frequencies = dataCache.freqs;
      double selectivity = 0;
      int accumFreq = 0;
      int index = openBitSet.nextSetBit(-1);
      while (index >= 0) {
        accumFreq += frequencies[index];
        index = openBitSet.nextSetBit(index);
      }
      int total = reader.maxDoc();
      selectivity = (double) accumFreq / (double) total;
      if (selectivity > 0.999) {
        selectivity = 1.0;
      }
      return selectivity;
    }
}
