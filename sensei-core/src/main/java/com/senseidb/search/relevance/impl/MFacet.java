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

import java.util.Set;

import com.browseengine.bobo.facets.data.MultiValueFacetDataCache;
import com.browseengine.bobo.facets.data.TermValueList;
import com.browseengine.bobo.util.BigNestedIntArray;

public abstract class MFacet
{
  protected MultiValueFacetDataCache _mDataCaches = null;
  protected BigNestedIntArray _nestedArray = null;
  protected TermValueList _mTermList = null;
  
  protected int[]   _buf = new int[1024];
  protected int     _length = 0;
  protected int[]   _weight = new int[1];

// for weighted multi-facet;
  protected BigNestedIntArray _weightArray = null;
  protected int[]   weightBuf = new int[1024];

  
  public MFacet(MultiValueFacetDataCache mDataCaches)
  {
    _mDataCaches = mDataCaches;
    _mTermList = _mDataCaches.valArray;
    _nestedArray = _mDataCaches._nestedArray;
  }

  public void refresh(int id)
  {
    _length = _nestedArray.getData(id, _buf);
  }
  
  public int size()
  {
    return _length;
  }
  
  abstract public boolean containsAll(Set set);
  abstract public boolean containsAny(Object set);
}
