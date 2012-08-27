/**
 * This software is licensed to you under the Apache License, Version 2.0 (the
 * "Apache License").
 *
 * LinkedIn's contributions are made under the Apache License. If you contribute
 * to the Software, the contributions will be deemed to have been made under the
 * Apache License, unless you expressly indicate otherwise. Please do not make any
 * contributions that would be inconsistent with the Apache License.
 *
 * You may obtain a copy of the Apache License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, this software
 * distributed under the Apache License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the Apache
 * License for the specific language governing permissions and limitations for the
 * software governed under the Apache License.
 *
 * © 2012 LinkedIn Corp. All Rights Reserved.  
 */
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
