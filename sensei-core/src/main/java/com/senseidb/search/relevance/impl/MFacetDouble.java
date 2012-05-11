package com.senseidb.search.relevance.impl;

import it.unimi.dsi.fastutil.doubles.DoubleOpenHashSet;

import java.util.Set;

import com.browseengine.bobo.facets.data.MultiValueFacetDataCache;
import com.browseengine.bobo.facets.data.TermDoubleList;

public class MFacetDouble extends MFacet
{

  public MFacetDouble(MultiValueFacetDataCache mDataCaches)
  {
    super(mDataCaches);
  }

  @Override
  public boolean containsAll(Set set)
  {
    throw new UnsupportedOperationException("not implemented yet");
  }

  public boolean containsAll(double[] target)
  {
    throw new UnsupportedOperationException("not implemented yet");
  }
  
  
  @Override
  public boolean containsAny(Object set)
  {
    DoubleOpenHashSet setDouble = (DoubleOpenHashSet)set;
    for(int i=0; i< this._length; i++)
      if( setDouble.contains(((TermDoubleList) _mTermList).getPrimitiveValue(_buf[i])) )
        return true;
              
    return false;
  }
  
  public boolean contains(double target)
  {
    for(int i=0; i< this._length; i++)
      if(((TermDoubleList) _mTermList).getPrimitiveValue(_buf[i]) == target)
        return true;
              
    return false;
  }
  
}
