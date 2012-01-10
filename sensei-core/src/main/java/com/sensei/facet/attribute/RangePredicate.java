package com.sensei.facet.attribute;

import org.apache.lucene.util.OpenBitSet;

import com.browseengine.bobo.api.BrowseSelection.ValueOperation;
import com.browseengine.bobo.facets.data.FacetDataCache;
import com.browseengine.bobo.facets.data.MultiValueFacetDataCache;

public class RangePredicate implements  FacetPredicate {
  private int startIndex = Integer.MAX_VALUE;
  private int endIndex = Integer.MIN_VALUE;
  private Range[] includes;
  private Range[] excludes;
  private final ValueOperation operation;
  private ThreadLocal<int[]> bufferHolder = new ThreadLocal<int[]>();
  private final String[] includeVals;
  private final String[] excludeVals;
  private final char separator;
  public RangePredicate(String[] includeVals, String[] excludeVals, ValueOperation operation, char separator) {
    this.includeVals = includeVals;
    this.excludeVals = excludeVals;    
    this.operation = operation;
    this.separator = separator;    
    
    if (operation == null) {
      throw new IllegalStateException("operation shoulnd be null");
    }
    int startIndex = Integer.MAX_VALUE;
    int endIndex = -1;
    
  }
  
  
  @Override
  public boolean evaluate(FacetDataCache cache, int docId) {
    MultiValueFacetDataCache multiDataCache = (MultiValueFacetDataCache) cache;    
    init(cache);
    if (bufferHolder.get() == null || bufferHolder.get().length < multiDataCache._nestedArray.getNumItems(docId)) {
      bufferHolder.set(new int[multiDataCache._nestedArray.getNumItems(docId) + 10]);
    }
    int[] buffer = bufferHolder.get();    
    int count = multiDataCache._nestedArray.getData(docId, buffer);
    OpenBitSet satisfied = new OpenBitSet(includes.length);
    for (int i  = 0; i < count; i++) {
      int valId = buffer[i];
      if (excludes.length > 0) {
        for (Range range : excludes) {
          if (range.inRange(valId)) {
            return false;
          }
        }          
      } 
      for (int rangeIndex = 0; rangeIndex < includes.length; rangeIndex++) {
        if (includes[rangeIndex].inRange(valId)) {
          if (operation == ValueOperation.ValueOperationOr) {
            return true;
          }
          satisfied.set(rangeIndex);
        } 
      }
    }
    if (satisfied.cardinality() != includes.length) {
      return false;
    }
    return true;
  }

  @Override
  public boolean evaluateValue(FacetDataCache cache, int valId) {
    init(cache);
    if (excludes.length > 0) {
      for (Range range : excludes) {
        if (range.inRange(valId)) {
          return false;
        }
      }
        
    } 
    if (includes.length == 0) {
      return true;
    }
    for (Range range : includes) {
      if (range.inRange(valId)) {
        return true;
      }     
    }  
    return false;
  }


  public void init(FacetDataCache cache) {
    if (includes == null) {
      MultiValueFacetDataCache multiDataCache = (MultiValueFacetDataCache) cache;    
      includes = Range.getRanges(multiDataCache, includeVals, separator);
      excludes = Range.getRanges(multiDataCache, excludeVals, separator);
      for (Range range : includes) {
        if (startIndex > range.start) startIndex = range.start;
        if (endIndex < range.end) endIndex = range.end;       
      }
    }
  }

  @Override
  public int valueStartIndex() {    
    return startIndex;
  }
  @Override
  public int valueEndIndex() {    
    return endIndex;
  }  
}
