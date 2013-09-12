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

package com.senseidb.facet.handler.loadable;

import com.senseidb.facet.FacetSpec;
import com.senseidb.facet.handler.loadable.data.BigSegmentedArray;
import com.senseidb.facet.handler.loadable.data.LazyBigIntArray;
import com.senseidb.facet.handler.FacetCountCollector;
import com.senseidb.facet.iterator.FacetIterator;
import com.senseidb.facet.iterator.loadable.LoadableFacetIterator;
import com.senseidb.facet.iterator.loadable.LoadableIntFacetIterator;
import com.senseidb.facet.iterator.loadable.LoadableLongFacetIterator;
import com.senseidb.facet.iterator.loadable.LoadableShortFacetIterator;
import com.senseidb.facet.iterator.loadable.TermIntList;
import com.senseidb.facet.iterator.loadable.TermLongList;
import com.senseidb.facet.iterator.loadable.TermShortList;

public abstract class LoadableFacetCountCollector extends FacetCountCollector
{
  protected BigSegmentedArray _count;
  protected int _countlength;

  protected final FacetSpec _spec;
  protected final FacetDataCache _dataCache;
  protected final BigSegmentedArray _array;

  private boolean _closed = false;

  public LoadableFacetCountCollector(FacetDataCache dataCache, FacetSpec spec)
  {
    _spec = spec;
    _dataCache=dataCache;
    _countlength = _dataCache.valArray.size();
    _count = new LazyBigIntArray(_countlength);
    _array = _dataCache.orderArray;
  }

  abstract public void collect(int docid);

  @Override
  public void close()
  {
    if (_closed)
    {
      return;
    }
    _closed = true;
  }

  /**
   * This function returns an Iterator to visit the facets in value order
   * @return	The Iterator to iterate over the facets in value order
   */
  public FacetIterator iterator()
  {
    if (_closed)
    {
      throw new IllegalStateException("This instance of _count collector was already closed");
    }
    if (_dataCache.valArray.getType().equals(Integer.class))
    {
      return new LoadableIntFacetIterator((TermIntList) _dataCache.valArray, _count, _countlength, false);
    } else if (_dataCache.valArray.getType().equals(Long.class))
    {
      return new LoadableLongFacetIterator((TermLongList) _dataCache.valArray, _count, _countlength, false);
    } else if (_dataCache.valArray.getType().equals(Short.class))
    {
      return new LoadableShortFacetIterator((TermShortList) _dataCache.valArray, _count, _countlength, false);
    } else
    return new LoadableFacetIterator(_dataCache.valArray, _count, _countlength, false);
  }
}
