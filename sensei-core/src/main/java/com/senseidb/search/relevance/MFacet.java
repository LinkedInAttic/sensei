package com.senseidb.search.relevance;

import java.util.Set;

import com.browseengine.bobo.facets.data.MultiValueFacetDataCache;
import com.browseengine.bobo.facets.data.TermValueList;
import com.browseengine.bobo.util.BigNestedIntArray;

public abstract class MFacet
{
  protected MultiValueFacetDataCache _mDataCaches = null;
  protected BigNestedIntArray _nestedArray = null;
  protected TermValueList _mTermList = null;
  
  
  protected int[]   buf = new int[1024];
  protected int     length = 0;
  
  public MFacet(MultiValueFacetDataCache mDataCaches)
  {
    _mDataCaches = mDataCaches;
    _mTermList = _mDataCaches.valArray;
    _nestedArray = _mDataCaches._nestedArray;
  }

  public void refresh(int id)
  {
    length = _nestedArray.getData(id, buf);
  }
  
  public int size()
  {
    return length;
  }
  
  abstract public boolean containsAll(Set set);
}
