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
    throw new UnsupportedOperationException("not implemented yet");
  }
  
  public boolean containsAll(int[] target)
  {
    throw new UnsupportedOperationException("not implemented yet");
  }
  
  
  public boolean contains(int target)
  {
    for(int i=0; i< this._length; i++)
      if(((TermIntList) _mTermList).getPrimitiveValue(_buf[i]) == target)
        return true;
              
    return false;
  }
}
