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

/**
 * @author "Xiaoyang Gu<xgu@linkedin.com>"
 * 
 */
public class LoadableShortFacetIterator extends NumericFacetIterator
{
  public TermShortList _valList;
  private BigSegmentedArray _counts;
  private int _countlength;
  private int _index;

  public LoadableShortFacetIterator(TermShortList valList, BigSegmentedArray countarray, int countlength,
                                    boolean zeroBased)
  {
    _valList = valList;
    _countlength = countlength;
    _counts = countarray;
    _index = -1;
    if (!zeroBased)
      _index++;
    _longFacet = TermShortList.VALUE_MISSING;
    this._count = 0;
  }

  public long nextLong()
  {
    if (++_index < _countlength)
    {
      _longFacet = _valList.getPrimitiveValue(_index);
      this._count = _counts.get(_index);
      return _longFacet;
    }
    _longFacet = TermShortList.VALUE_MISSING;
    this._count = 0;
    return _longFacet;
  }

}
