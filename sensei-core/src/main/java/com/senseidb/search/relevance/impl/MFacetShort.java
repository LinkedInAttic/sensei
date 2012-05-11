package com.senseidb.search.relevance.impl;

import it.unimi.dsi.fastutil.shorts.ShortOpenHashSet;

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
    throw new UnsupportedOperationException("not implemented yet");
  }

  public boolean containsAll(short[] target)
  {
    throw new UnsupportedOperationException("not implemented yet");
  }
  
  @Override
  public boolean containsAny(Object set)
  {
    ShortOpenHashSet setShort = (ShortOpenHashSet)set;
    for(int i=0; i< this._length; i++)
      if( setShort.contains(((TermShortList) _mTermList).getPrimitiveValue(_buf[i])) )
        return true;
              
    return false;
  }
  
  public boolean contains(short target)
  {
    for(int i=0; i< this._length; i++)
      if(((TermShortList) _mTermList).getPrimitiveValue(_buf[i]) == target)
        return true;
              
    return false;
  }
  
}
