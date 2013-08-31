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

import com.senseidb.facet.Facet;
import com.senseidb.facet.FacetIterator;
import com.senseidb.facet.FacetSpec;
import com.senseidb.facet.data.BigSegmentedArray;
import com.senseidb.facet.data.IntBoundedPriorityQueue;
import com.senseidb.facet.data.LazyBigIntArray;
import com.senseidb.facet.handler.loadable.iterator.LoadableDoubleFacetIterator;
import com.senseidb.facet.handler.loadable.iterator.LoadableFacetIterator;
import com.senseidb.facet.handler.loadable.iterator.LoadableFloatFacetIterator;
import com.senseidb.facet.handler.loadable.iterator.LoadableIntFacetIterator;
import com.senseidb.facet.handler.loadable.iterator.LoadableLongFacetIterator;
import com.senseidb.facet.handler.loadable.iterator.LoadableShortFacetIterator;
import com.senseidb.facet.termlist.TermDoubleList;
import com.senseidb.facet.termlist.TermFloatList;
import com.senseidb.facet.termlist.TermIntList;
import com.senseidb.facet.termlist.TermLongList;
import com.senseidb.facet.termlist.TermShortList;
import com.senseidb.facet.termlist.TermValueList;
import com.senseidb.facet.handler.ComparatorFactory;
import com.senseidb.facet.handler.FacetCountCollector;
import com.senseidb.facet.handler.FieldValueAccessor;
import com.senseidb.facet.iterator.FacetHitcountComparatorFactory;
import com.senseidb.facet.data.IntComparator;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public abstract class LoadableFacetCountCollector implements FacetCountCollector
{
  protected BigSegmentedArray _count;
  protected int _countlength;

  protected final FacetSpec _spec;
  protected final FacetDataCache _dataCache;
  protected final BigSegmentedArray _array;

  private final String _name;
  private boolean _closed = false;

  public LoadableFacetCountCollector(String name, FacetDataCache dataCache, FacetSpec spec)
  {
    _spec = spec;
    _name = name;
    _dataCache=dataCache;
    _countlength = _dataCache.valArray.size();
    _count = new LazyBigIntArray(_countlength);
    _array = _dataCache.orderArray;
  }

  public String getName()
  {
    return _name;
  }

  abstract public void collect(int docid);

  public Facet getFacet(String value)
  {
    if (_closed)
    {
      throw new IllegalStateException("This instance of count collector for " + _name + " was already closed");
    }
    Facet facet = null;
    int index=_dataCache.valArray.indexOf(value);
    if (index >=0 ){
      facet = new Facet(_dataCache.valArray.get(index),_count.get(index));
    }
    else{
      facet = new Facet(_dataCache.valArray.format(value),0);
    }
    return facet; 
  }

  public static List<Facet> getFacets(FacetSpec ospec, BigSegmentedArray count, int countlength, final TermValueList<?> valList){
	  if (ospec!=null)
	    {
	      int minCount=ospec.getMinHitCount();
	      int max=ospec.getMaxCount();
	      if (max <= 0)
          max=countlength;


	      List<Facet> facetColl;
	      FacetSpec.FacetSortSpec sortspec = ospec.getOrderBy();
	      if (sortspec == FacetSpec.FacetSortSpec.OrderValueAsc)
	      {
	        facetColl=new ArrayList<Facet>(max);
	        for (int i = 1; i < countlength;++i) // exclude zero
	        {
	          int hits=count.get(i);
	          if (hits>=minCount)
	          {
              Facet facet=new Facet(valList.get(i),hits);
	            facetColl.add(facet);
	          }
	          if (facetColl.size()>=max) break;
	        }
	      }
	      else //if (sortspec == FacetSortSpec.OrderHitsDesc)
	      {
	        ComparatorFactory comparatorFactory;
	        if (sortspec == FacetSpec.FacetSortSpec.OrderHitsDesc){
	          comparatorFactory = new FacetHitcountComparatorFactory();
	        }
	        else{
	          comparatorFactory = ospec.getCustomComparatorFactory();
	        }

	        if (comparatorFactory == null){
	          throw new IllegalArgumentException("facet comparator factory not specified");
	        }

	        final IntComparator comparator = comparatorFactory.newComparator(new FieldValueAccessor(){

	          public String getFormatedValue(int index) {
	            return valList.get(index);
	          }

	          public Object getRawValue(int index) {
	            return valList.getRawValue(index);
	          }

	        }, count);
	        facetColl=new LinkedList<Facet>();
	        final int forbidden = -1;
	        IntBoundedPriorityQueue pq=new IntBoundedPriorityQueue(comparator,max, forbidden);

	        for (int i=1;i<countlength;++i)
	        {
	          int hits=count.get(i);
	          if (hits>=minCount)
	          {
	            pq.offer(i);
	          }
	        }

	        int val;
	        while((val = pq.pollInt()) != forbidden)
	        {
	          Facet facet=new Facet(valList.get(val),count.get(val));
	          ((LinkedList<Facet>)facetColl).addFirst(facet);
	        }
	      }
	      return facetColl;
	    }
	    else
	    {
	      return FacetCountCollector.EMPTY_FACET_LIST;
	    }
  }

  public List<Facet> getTopFacets() {
    if (_closed)
    {
      throw new IllegalStateException("This instance of count collector for " + _name + " was already closed");
    }
    
    return getFacets(_spec,_count, _countlength, _dataCache.valArray);
    
  }

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
      throw new IllegalStateException("This instance of count collector for '" + _name + "' was already closed");
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
    } else if (_dataCache.valArray.getType().equals(Float.class))
    {
      return new LoadableFloatFacetIterator((TermFloatList) _dataCache.valArray, _count, _countlength, false);
    } else if (_dataCache.valArray.getType().equals(Double.class))
    {
      return new LoadableDoubleFacetIterator((TermDoubleList) _dataCache.valArray, _count, _countlength, false);
    } else
    return new LoadableFacetIterator(_dataCache.valArray, _count, _countlength, false);
  }
}
