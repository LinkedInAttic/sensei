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

package com.senseidb.facet.iterator.loadable;

import com.senseidb.facet.handler.loadable.data.BigSegmentedArray;
import com.senseidb.facet.iterator.NumericFacetIterator;

public class LoadableIntFacetIterator extends NumericFacetIterator
{
  public TermIntList _valList;
  private BigSegmentedArray _counts;
  private int _countlength;
  private int _index;

  public LoadableIntFacetIterator(TermIntList valList, BigSegmentedArray countarray, int countlength, boolean zeroBased)
  {
    _valList = valList;
    _counts = countarray;
    _countlength = countlength;
    _index = -1;
    if(!zeroBased)
      _index++;
    _longFacet = NumericFacetIterator.VALUE_MISSING;
    this._count = 0;
  }

  public long nextLong()
  {
    if(++_index < _countlength)
    {
      _longFacet = _valList.getPrimitiveValue(_index);
      this._count = _counts.get(_index);
      return _longFacet;
    }
    _longFacet = TermIntList.VALUE_MISSING;
    this._count = 0;
    return _longFacet;
  }
}
