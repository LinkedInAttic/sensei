package com.senseidb.search.relevance;

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
    for(int i=0; i< this.length; i++)
      if(set.contains(((TermStringList) _mTermList).get(buf[i])))
        return true;
              
    return false;
  }

  public boolean containsAll(String[] target)
  {
    return false;
  }
  
  
  public boolean contains(String target)
  {
    for(int i=0; i< this.length; i++)
      if(((TermStringList) _mTermList).get(buf[i]).equals(target))
        return true;
              
    return false;
  }
  
}
