package com.sensei.facet.attribute;

import com.browseengine.bobo.facets.data.FacetDataCache;

public interface FacetPredicate {
  public boolean evaluate(FacetDataCache cache, int docId);
  public boolean evaluateValue(FacetDataCache cache, int valId);
  public int valueStartIndex();
  public int valueEndIndex();
  public void init(FacetDataCache facetDataCache);
  
  public final FacetPredicate TRUE = new DefaultFacetPredicate(true);
  public final FacetPredicate FALSE = new DefaultFacetPredicate(false);
  
  public static class DefaultFacetPredicate implements  FacetPredicate {
  private final boolean flag;

  public DefaultFacetPredicate(boolean flag) {
    this.flag = flag;
    // TODO Auto-generated constructor stub
  }
    @Override
    public boolean evaluate(FacetDataCache cache, int docId) {      
      return flag;
    }

    @Override
    public boolean evaluateValue(FacetDataCache cache, int valId) {     
      return flag;
    }

    @Override
    public int valueStartIndex() {      
      return 0;
    }

    @Override
    public int valueEndIndex() {      
      return 0;
    }
    @Override
    public void init(FacetDataCache facetDataCache) {
      // TODO Auto-generated method stub
      
    }
    
  }
}
