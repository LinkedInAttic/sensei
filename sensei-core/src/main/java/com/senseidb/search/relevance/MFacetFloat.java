package com.senseidb.search.relevance;

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
  
  
  public boolean contains(float target)
  {
    for(int i=0; i< this.length; i++)
      if(((TermFloatList) _mTermList).getPrimitiveValue(buf[i]) == target)
        return true;
              
    return false;
  }

}
