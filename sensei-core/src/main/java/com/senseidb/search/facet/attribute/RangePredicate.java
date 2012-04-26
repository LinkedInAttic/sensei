package com.senseidb.search.facet.attribute;

import com.linkedin.bobo.facets.data.FacetDataCache;
import com.linkedin.bobo.facets.data.MultiValueFacetDataCache;

public class RangePredicate implements  FacetPredicate {
 
  
  private final String value;
  private final char separator;
  private MultiValueFacetDataCache lastDataCache;
  private Range range;
  private int[] buffer;
  private final Range getRange(FacetDataCache cache) {
    if (cache == lastDataCache) {
      return range;
    }    
    lastDataCache = (MultiValueFacetDataCache) cache;   
    range = Range.getRange(lastDataCache, value, separator);   
    buffer = new int[lastDataCache._nestedArray.getMaxItems()];
    return range;
  }  
  
  public RangePredicate(String val, char separator) {
    value = val;
    this.separator = separator;   
  }  
  
  @Override
  public boolean evaluate(FacetDataCache cache, int docId) {
    if (cache != lastDataCache) {
      getRange(cache);
    }
   return lastDataCache._nestedArray.containsValueInRange(docId, range.start, range.end);
  }

  @Override
  public boolean evaluateValue(FacetDataCache cache, int valId) {
    if (cache != lastDataCache) {
      getRange(cache);
    }
    return valId >= range.start && valId < range.end;
  } 

  @Override
  public int valueStartIndex(FacetDataCache cache) {    
    if (cache != lastDataCache) {
      getRange(cache);
    }
    return range.start;
  }
  @Override
  public int valueEndIndex(FacetDataCache cache) {    
    return range.end;
  } 
}
