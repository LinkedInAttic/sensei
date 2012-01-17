package com.senseidb.search.facet.attribute;

import com.browseengine.bobo.facets.data.FacetDataCache;

public interface FacetPredicate {
  public boolean evaluate(FacetDataCache cache, int docId);
  public boolean evaluateValue(FacetDataCache cache, int valId);
  public int valueStartIndex(FacetDataCache cache);
  public int valueEndIndex(FacetDataCache cache); 
  
  
}
