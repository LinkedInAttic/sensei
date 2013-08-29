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

package com.senseidb.facet.iterator;

import com.senseidb.facet.FacetIterator;
import com.senseidb.facet.data.TermValueList;
import com.senseidb.facet.data.BigSegmentedArray;

/**
 * @author nnarkhed
 *
 */
public class DefaultFacetIterator extends FacetIterator {

  private TermValueList _valList;
  private BigSegmentedArray _count;
  private int _countlength;
  private int _index;
  private int _lastIndex;

  public DefaultFacetIterator(TermValueList valList, BigSegmentedArray counts, int countlength, boolean zeroBased)
  {
    _valList = valList;
    _count = counts;
    _countlength = countlength;
    _index = -1;
    _lastIndex = _countlength - 1;
    if(!zeroBased)
      _index++;
    facet = null;
    count = 0;
  }


  /* (non-Javadoc)
   * @see java.util.Iterator#hasNext()
   */
  public boolean hasNext() {
    return (_index < _lastIndex);
  }

  /* (non-Javadoc)
   * @see java.util.Iterator#next()
   */
  public Comparable next() {
    _index++;
    facet = (Comparable)_valList.getRawValue(_index);
    count = _count.get(_index);
    return format(facet);
  }

  /* (non-Javadoc)
   * @see java.util.Iterator#remove()
   */
  public void remove() {
    throw new UnsupportedOperationException("remove() method not supported for Facet Iterators");
  }

  /* (non-Javadoc)
   * @see com.browseengine.bobo.api.FacetIterator#next(int)
   */
  public Comparable next(int minHits)
  {
    while(++_index < _countlength)
    {
      if(_count.get(_index) >= minHits)
      {
    	facet = (Comparable)_valList.getRawValue(_index);
        count = _count.get(_index);
        return format(facet);
      }
    }
    facet = null;
    count = 0;
    return null;    
  }


  @Override
  public String format(Object val)
  {
    return _valList.format(val);
  }
  
}
