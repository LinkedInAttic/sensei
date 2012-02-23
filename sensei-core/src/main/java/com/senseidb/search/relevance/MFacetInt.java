package com.senseidb.search.relevance;

import java.util.Set;

import com.browseengine.bobo.facets.data.MultiValueFacetDataCache;
import com.browseengine.bobo.facets.data.TermIntList;

public class MFacetInt extends MFacet
{

  public MFacetInt(MultiValueFacetDataCache mDataCaches)
  {
    super(mDataCaches);
  }

  @Override
  public boolean containsAll(Set set)
  {
    for(int i=0; i< this.length; i++)
      if(set.contains(((TermIntList) _mTermList).getPrimitiveValue(buf[i])))
        return true;
              
    return false;
  }
  
  public boolean containsAll(int[] target)
  {
    return false;
  }
  
  
  public boolean contains(int target)
  {
    for(int i=0; i< this.length; i++)
      if(((TermIntList) _mTermList).getPrimitiveValue(buf[i]) == target)
        return true;
              
    return false;
  }
}
