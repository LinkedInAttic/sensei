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
