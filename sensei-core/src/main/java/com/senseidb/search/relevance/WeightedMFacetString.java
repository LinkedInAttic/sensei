package com.senseidb.search.relevance;

import com.linkedin.bobo.facets.data.MultiValueFacetDataCache;
import com.linkedin.bobo.facets.data.MultiValueWithWeightFacetDataCache;
import com.linkedin.bobo.facets.data.TermStringList;

public class WeightedMFacetString extends MFacetString implements WeightedMFacet
{

  public WeightedMFacetString(MultiValueFacetDataCache mDataCaches)
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

  public boolean hasWeight(String target){
    
    for(int i=0; i< this._length; i++)
      if(((TermStringList) _mTermList).get(_buf[i]).equals(target))
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
}
