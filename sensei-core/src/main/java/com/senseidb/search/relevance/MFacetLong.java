package com.senseidb.search.relevance;

import java.util.Set;

import com.browseengine.bobo.facets.data.MultiValueFacetDataCache;
import com.browseengine.bobo.facets.data.TermLongList;

public class MFacetLong extends MFacet
{

  public MFacetLong(MultiValueFacetDataCache mDataCaches)
  {
    super(mDataCaches);
  }

  @Override
  public boolean containsAll(Set set)
  {
    for(int i=0; i< this.length; i++)
      if(set.contains(((TermLongList) _mTermList).getPrimitiveValue(buf[i])))
        return true;
              
    return false;
  }
  
  public boolean containsAll(long[] target)
  {
    return false;
  }
  
  
  public boolean contains(long target)
  {
    for(int i=0; i< this.length; i++)
      if(((TermLongList) _mTermList).getPrimitiveValue(buf[i]) == target)
        return true;
              
    return false;
  }

}
