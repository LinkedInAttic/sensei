package com.senseidb.search.facet.attribute;

import com.browseengine.bobo.facets.data.MultiValueFacetDataCache;

public class Range {
  int start;
  int end;
  public Range(int start, int end) {      
    this.start = start;
    this.end = end;
  }
  public boolean inRange(int val) {
    return val >= start && val < end;
  }
  public static Range[] getRanges(MultiValueFacetDataCache cache, String[] vals, char separator) {
    Range[] ret = new Range[vals.length];
    for (int i = 0; i < vals.length; i++) {
      ret[i] = getRange(cache, vals[i], separator);
    }
    return ret;
  }
    
    public static Range getRange(MultiValueFacetDataCache cache, String val, char separator) {
      int start = cache.valArray.indexOf(val + separator);
      if (start < 0) {
        start = (-1)*start - 1;
      }
      int end = cache.valArray.indexOf(val + (char) (separator + 1));
      if (end < 0) {
        end = (-1)*end - 1;
      }
      return new Range(start, end);
    }
    @Override
    public String toString() {
      return "[start=" + start + ", end=" + end + "]";
    }
    
}