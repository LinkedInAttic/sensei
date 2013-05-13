package com.senseidb.search.relevance.impl;

import com.browseengine.bobo.facets.data.MultiValueFacetDataCache;
import com.browseengine.bobo.facets.data.MultiValueWithWeightFacetDataCache;
import com.browseengine.bobo.facets.data.TermFloatList;

public class WeightedMFacetFloat extends MFacetFloat implements WeightedMFacet
{

  public WeightedMFacetFloat(MultiValueFacetDataCache mDataCaches)
  {
    super(mDataCaches);
    
    MultiValueWithWeightFacetDataCache wmDataCaches = (MultiValueWithWeightFacetDataCache) mDataCaches;
    _weightArray = wmDataCaches._weightArray;
    weightBuf = new int[1024];
  }

  @Override
  public void refresh(int id)
  {
    _length = _nestedArray.getData(id, _buf);
    _weightArray.getData(id, weightBuf);
  }

  public boolean hasWeight(float target){
    
    for(int i=0; i< this._length; i++)
      if(((TermFloatList) _mTermList).getPrimitiveValue(_buf[i]) == target)
      {
        _weight[0] = weightBuf[i];
        return true;
      }
              
    return false;
  }

  @Override
  public int getWeight()
  {
    return _weight[0];
  }
  
  public int getWeight(float target)
  {
    if(hasWeight(target))
      return _weight[0];
    else
      return Integer.MIN_VALUE;
  }
}
