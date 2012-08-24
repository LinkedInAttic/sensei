package com.senseidb.search.relevance.impl;

import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;

import java.util.Set;

import com.browseengine.bobo.facets.data.MultiValueFacetDataCache;
import com.browseengine.bobo.facets.data.TermStringList;

public class MFacetString extends MFacet
{

  public MFacetString(MultiValueFacetDataCache mDataCaches)
  {
    super(mDataCaches);
  }

  @Override
  public boolean containsAll(Set set)
  {
    throw new UnsupportedOperationException("not implemented yet");
  }

  public boolean containsAll(String[] target)
  {
    throw new UnsupportedOperationException("not implemented yet");
  }
  
  @Override
  public boolean containsAny(Object set)
  {
    ObjectOpenHashSet setString = (ObjectOpenHashSet)set;
    for(int i=0; i< this._length; i++)
      if( setString.contains(((TermStringList) _mTermList).get(_buf[i])) )
        return true;
              
    return false;
  }
  
  public boolean contains(String target)
  {
    for(int i=0; i< this._length; i++)
      if(((TermStringList) _mTermList).get(_buf[i]).equals(target))
        return true;
              
    return false;
  }
  
}
