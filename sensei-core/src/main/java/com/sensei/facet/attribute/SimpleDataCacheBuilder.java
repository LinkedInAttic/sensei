package com.sensei.facet.attribute;

import com.browseengine.bobo.api.BoboIndexReader;
import com.browseengine.bobo.facets.data.FacetDataCache;
import com.browseengine.bobo.facets.filter.AdaptiveFacetFilter.FacetDataCacheBuilder;

public class SimpleDataCacheBuilder implements FacetDataCacheBuilder{
  private String name;
  public SimpleDataCacheBuilder( String name) {      
    this.name = name;
    
  }
  public FacetDataCache build(BoboIndexReader reader) {
    return (FacetDataCache) reader.getFacetData(name);
  }
  public String getName() {
    return name;
  }   
}
