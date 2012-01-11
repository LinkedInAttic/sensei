package com.sensei.facet.attribute;

import java.util.Map;
import java.util.WeakHashMap;

import org.apache.lucene.util.OpenBitSet;

import com.browseengine.bobo.api.BrowseSelection.ValueOperation;
import com.browseengine.bobo.facets.data.FacetDataCache;
import com.browseengine.bobo.facets.data.MultiValueFacetDataCache;

public class RangePredicate implements  FacetPredicate {
 
  private final ValueOperation operation;
  private ThreadLocal<int[]> bufferHolder = new ThreadLocal<int[]>();
  private final String[] includeVals;
  private final String[] excludeVals;
  private final char separator;
  private FacetDataCache lastDataCache;
  private RangeHolder lastHolder;
  private Map<FacetDataCache, RangeHolder> holders = new WeakHashMap<FacetDataCache, RangeHolder>();
  
  private final RangeHolder getRangeHolder(FacetDataCache cache) {
    if (cache == lastDataCache) {
      return lastHolder;
    }
    RangeHolder holder = holders.get(cache);
    if (holder == null) {
      holder = createRange(cache);
      holders.put(cache, holder);
    }
    lastDataCache = cache;
    lastHolder = holder;
    return holder;
  }
  
  
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
    RangeHolder rangeHolder = getRangeHolder(cache);
    if (bufferHolder.get() == null || bufferHolder.get().length < multiDataCache._nestedArray.getNumItems(docId)) {
      bufferHolder.set(new int[multiDataCache._nestedArray.getNumItems(docId) + 10]);
    }
    int[] buffer = bufferHolder.get();    
    int count = multiDataCache._nestedArray.getData(docId, buffer);
    OpenBitSet satisfied = new OpenBitSet(rangeHolder.includes.length);
    for (int i  = 0; i < count; i++) {
      int valId = buffer[i];
      if (rangeHolder.excludes.length > 0) {
        for (Range range : rangeHolder.excludes) {
          if (range.inRange(valId)) {
            return false;
          }
        }          
      } 
      for (int rangeIndex = 0; rangeIndex < rangeHolder.includes.length; rangeIndex++) {
        if (rangeHolder.includes[rangeIndex].inRange(valId)) {
          if (operation == ValueOperation.ValueOperationOr) {
            return true;
          }
          satisfied.set(rangeIndex);
        } 
      }
    }
    if (satisfied.cardinality() != rangeHolder.includes.length) {
      return false;
    }
    return true;
  }

  @Override
  public boolean evaluateValue(FacetDataCache cache, int valId) {
    RangeHolder rangeHolder = getRangeHolder(cache);
    if (rangeHolder.excludes.length > 0) {
      for (Range range : rangeHolder.excludes) {
        if (range.inRange(valId)) {
          return false;
        }
      }
        
    } 
    if (rangeHolder.includes.length == 0) {
      return true;
    }
    for (Range range : rangeHolder.includes) {
      if (range.inRange(valId)) {
        return true;
      }     
    }  
    return false;
  }


 

  @Override
  public int valueStartIndex(FacetDataCache cache) {    
    return getRangeHolder(cache).startIndex;
  }
  @Override
  public int valueEndIndex(FacetDataCache cache) {    
    return getRangeHolder(cache).endIndex;
  }  
  
  
  private RangeHolder createRange(FacetDataCache cache) {    
    RangeHolder ret = new RangeHolder();      
    MultiValueFacetDataCache multiDataCache = (MultiValueFacetDataCache) cache;    
    ret.includes = Range.getRanges(multiDataCache, includeVals, separator);
    ret.excludes = Range.getRanges(multiDataCache, excludeVals, separator);
    for (Range range : ret.includes) {
      if (ret.startIndex > range.start) ret.startIndex = range.start;
      if (ret.endIndex < range.end) ret.endIndex = range.end;       
    }
   return ret;

}
  public static class RangeHolder {
    public Range[] includes;
    public Range[] excludes;
    public int startIndex = Integer.MAX_VALUE;
    public int endIndex = Integer.MIN_VALUE;
  }
  
  
}
