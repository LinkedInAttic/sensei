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

import com.senseidb.facet.data.TermIntList;
import com.senseidb.facet.data.BigSegmentedArray;

import java.util.NoSuchElementException;

public class DefaultIntFacetIterator extends IntFacetIterator
{

  public TermIntList _valList;
  private BigSegmentedArray _count;
  private int _countlength;
  private int _countLengthMinusOne;
  private int _index;

  public DefaultIntFacetIterator(TermIntList valList, BigSegmentedArray countarray, int countlength, boolean zeroBased)
  {
    _valList = valList;
    _count = countarray;
    _countlength = countlength;
    _countLengthMinusOne = countlength-1;
    _index = -1;
    if(!zeroBased)
      _index++;
    facet = TermIntList.VALUE_MISSING;
    count = 0;
  }

  /* (non-Javadoc)
   * @see com.browseengine.bobo.api.FacetIterator#getFacet()
   */
  public String getFacet() {
    if (facet == -1) return null;
    return _valList.format(facet);
  }
  public String format(int val)
  {
    return _valList.format(val);
  }
  public String format(Object val)
  {
    return _valList.format(val);
  }
  /* (non-Javadoc)
   * @see com.browseengine.bobo.api.FacetIterator#getFacetCount()
   */
  public int getFacetCount() {
    return count;
  }

  /* (non-Javadoc)
   * @see java.util.Iterator#hasNext()
   */
  public boolean hasNext() {
    return (_index < _countLengthMinusOne);
  }

  /* (non-Javadoc)
   * @see java.util.Iterator#next()
   */
  public String next() {
    if((_index >= 0) && (_index >= _countLengthMinusOne))
      throw new NoSuchElementException("No more facets in this iteration");
    _index++;
    facet = _valList.getPrimitiveValue(_index);
    count = _count.get(_index);
    return _valList.get(_index);
  }

  /* (non-Javadoc)
   * @see com.browseengine.bobo.api.IntFacetIterator#nextInt()
   */
  public int nextInt()
  {
    if(_index >= _countLengthMinusOne)
      throw new NoSuchElementException("No more facets in this iteration");
    _index++;
    facet = _valList.getPrimitiveValue(_index);
    count = _count.get(_index);
    return facet;
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
  public String next(int minHits)
  {
    while(++_index < _countlength)
    {
      if(_count.get(_index) >= minHits)
      {
        facet = _valList.getPrimitiveValue(_index);
        count = _count.get(_index);
        return _valList.format(facet);
      }
    }
    facet = TermIntList.VALUE_MISSING;
    count = 0;
    return null;    
  }
  /* (non-Javadoc)
   * @see com.browseengine.bobo.api.IntFacetIterator#nextInt(int)
   */
  public int nextInt(int minHits)
  {
    while(++_index < _countlength)
    {
      if(_count.get(_index) >= minHits)
      {
        facet = _valList.getPrimitiveValue(_index);
        count = _count.get(_index);
        return facet;
      }
    }
    facet = TermIntList.VALUE_MISSING;
    count = 0;
    return facet;    
  }
}
