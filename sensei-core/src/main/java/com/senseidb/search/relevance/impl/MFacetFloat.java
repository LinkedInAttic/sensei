package com.senseidb.search.relevance.impl;

import it.unimi.dsi.fastutil.floats.FloatOpenHashSet;

import java.util.Set;

import com.browseengine.bobo.facets.data.MultiValueFacetDataCache;
import com.browseengine.bobo.facets.data.TermFloatList;

public class MFacetFloat extends MFacet
{

  public MFacetFloat(MultiValueFacetDataCache mDataCaches)
  {
    super(mDataCaches);
  }

  @Override
  public boolean containsAll(Set set)
  {
    throw new UnsupportedOperationException("not implemented yet");
  }
  
  public boolean containsAll(float[] target)
  {
    throw new UnsupportedOperationException("not implemented yet");
  }
  
  @Override
  public boolean containsAny(Object set)
  {
    FloatOpenHashSet setFloat = (FloatOpenHashSet)set;
    for(int i=0; i< this._length; i++)
      if( setFloat.contains(((TermFloatList) _mTermList).getPrimitiveValue(_buf[i])) )
        return true;
              
    return false;
  }
  
  public boolean contains(float target)
  {
    for(int i=0; i< this._length; i++)
      if(((TermFloatList) _mTermList).getPrimitiveValue(_buf[i]) == target)
        return true;
              
    return false;
  }

}
