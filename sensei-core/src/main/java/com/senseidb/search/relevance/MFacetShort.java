package com.senseidb.search.relevance;

import java.util.Set;

import com.browseengine.bobo.facets.data.MultiValueFacetDataCache;
import com.browseengine.bobo.facets.data.TermShortList;

public class MFacetShort extends MFacet
{

  public MFacetShort(MultiValueFacetDataCache mDataCaches)
  {
    super(mDataCaches);
  }

  @Override
  public boolean containsAll(Set set)
  {
    for(int i=0; i< this.length; i++)
      if(set.contains(((TermShortList) _mTermList).getPrimitiveValue(buf[i])))
        return true;
              
    return false;
  }

  public boolean containsAll(short[] target)
  {
    
    return false;
  }
  
  
  public boolean contains(short target)
  {
    for(int i=0; i< this.length; i++)
      if(((TermShortList) _mTermList).getPrimitiveValue(buf[i]) == target)
        return true;
              
    return false;
  }
  
}
