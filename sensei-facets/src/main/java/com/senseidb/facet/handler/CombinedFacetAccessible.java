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

package com.senseidb.facet.handler;


import com.senseidb.facet.Facet;
import com.senseidb.facet.iterator.DoubleFacetIterator;
import com.senseidb.facet.FacetAccessible;
import com.senseidb.facet.FacetIterator;
import com.senseidb.facet.FacetSpec;
import com.senseidb.facet.iterator.FloatFacetIterator;
import com.senseidb.facet.iterator.IntFacetIterator;
import com.senseidb.facet.iterator.LongFacetIterator;
import com.senseidb.facet.iterator.ShortFacetIterator;
import com.senseidb.facet.iterator.CombinedDoubleFacetIterator;
import com.senseidb.facet.iterator.CombinedFacetIterator;
import com.senseidb.facet.iterator.CombinedFloatFacetIterator;
import com.senseidb.facet.iterator.CombinedIntFacetIterator;
import com.senseidb.facet.iterator.CombinedLongFacetIterator;
import com.senseidb.facet.iterator.CombinedShortFacetIterator;
import org.apache.lucene.util.PriorityQueue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;


/**
 * @author nnarkhed
 *
 */
public class CombinedFacetAccessible implements FacetAccessible
{
  protected final List<FacetAccessible> _list;
  protected final FacetSpec _fspec;
  protected boolean _closed;
  
  public CombinedFacetAccessible(FacetSpec fspec,List<FacetAccessible> list)
  {
    _list = list;
    _fspec = fspec;
  }

  public String toString() 
  {
    return "_list:"+_list+" _fspec:"+_fspec;
  }

  public Facet getFacet(String value)
  {
    if (_closed)
    {
      throw new IllegalStateException("This instance of count collector was already closed");
    }
    int sum=-1;
    String foundValue=null;
    if (_list!=null)
    {
      for (FacetAccessible facetAccessor : _list)
      {
        Facet facet = facetAccessor.getFacet(value);
        if (facet!=null)
        {
          foundValue = facet.getValue();
          if (sum==-1) sum=facet.getFacetValueHitCount();
          else sum+=facet.getFacetValueHitCount();
        }
      }
    }
    if (sum==-1) return null;
    return new Facet(foundValue,sum);
  }

  public List<Facet> getTopFacets()
  {
    if (_closed)
    {
      throw new IllegalStateException("This instance of count collector was already closed");
    }
    int maxCnt = _fspec.getMaxCount();
    if(maxCnt <= 0)
      maxCnt = Integer.MAX_VALUE;
    int minHits = _fspec.getMinHitCount();
    LinkedList<Facet> list = new LinkedList<Facet>();

    int cnt = 0;
    Comparable facet = null;
    FacetIterator iter = this.iterator();
    Comparator<Facet> comparator;
    if (FacetSpec.FacetSortSpec.OrderValueAsc.equals(_fspec.getOrderBy()))
    {
      while((facet = iter.next(minHits)) != null) 
      {
        // find the next facet whose combined hit count obeys minHits
        list.add(new Facet(String.valueOf(facet), iter.count));
        if(++cnt >= maxCnt) break;                  
      }
    }
    else if(FacetSpec.FacetSortSpec.OrderHitsDesc.equals(_fspec.getOrderBy()))
    {
      comparator = new Comparator<Facet>()
      {
        public int compare(Facet f1, Facet f2)
        {
          int val=f2.getFacetValueHitCount() - f1.getFacetValueHitCount();
          if (val==0)
          {
            val = (f1.getValue().compareTo(f2.getValue()));
          }
          return val;
        }
      };       
      if(maxCnt != Integer.MAX_VALUE)
      {
        // we will maintain a min heap of size maxCnt
        // Order by hits in descending order and max count is supplied
        PriorityQueue queue = createPQ(maxCnt, comparator);
        int qsize = 0;
        while( (qsize < maxCnt) && ((facet = iter.next(minHits)) != null) )
        {
          queue.add(new Facet(String.valueOf(facet), iter.count));
          qsize++;
        }
        if(facet != null)
        {
          Facet rootFacet = (Facet)queue.top();
          minHits = rootFacet.getFacetValueHitCount() + 1;
          // facet count less than top of min heap, it will never be added 
          while(((facet = iter.next(minHits)) != null))
          {
            rootFacet.setValue(String.valueOf(facet));
            rootFacet.setFacetValueHitCount(iter.count);
            rootFacet = (Facet) queue.updateTop();
            minHits = rootFacet.getFacetValueHitCount() + 1;
          }
        }
        // at this point, queue contains top maxCnt facets that have hitcount >= minHits
        while(qsize-- > 0)
        {
          // append each entry to the beginning of the facet list to order facets by hits descending
          list.addFirst((Facet) queue.pop());
        }
      }
      else
      {
        // no maxCnt specified. So fetch all facets according to minHits and sort them later
        while((facet = iter.next(minHits)) != null)
          list.add(new Facet(String.valueOf(facet), iter.count));
        Collections.sort(list, comparator);
      }
    }
    else // FacetSortSpec.OrderByCustom.equals(_fspec.getOrderBy()
    {
      comparator = _fspec.getCustomComparatorFactory().newComparator();
      if(maxCnt != Integer.MAX_VALUE)
      {
        PriorityQueue queue = createPQ(maxCnt, comparator);
        Facet browseFacet = new Facet();
        int qsize = 0;
        while( (qsize < maxCnt) && ((facet = iter.next(minHits)) != null) )
        {
          queue.add(new Facet(String.valueOf(facet), iter.count));
          qsize++;
        }
        if(facet != null)
        {
          while((facet = iter.next(minHits)) != null)
          {
            // check with the top of min heap
            browseFacet.setFacetValueHitCount(iter.count);
            browseFacet.setValue(String.valueOf(facet));
            browseFacet = (Facet)queue.insertWithOverflow(browseFacet);
          }
        }
        // remove from queue and add to the list
        while(qsize-- > 0)
          list.addFirst((Facet)queue.pop());
      }
      else 
      {
        // order by custom but no max count supplied
        while((facet = iter.next(minHits)) != null)
          list.add(new Facet(String.valueOf(facet), iter.count));
        Collections.sort(list, comparator);
      }
    }
    return list;
  }

  private PriorityQueue createPQ(final int max, final Comparator<Facet> comparator)
  {
    PriorityQueue queue = new PriorityQueue(max)
    {
      @Override
      protected boolean lessThan(Object arg0, Object arg1)
      {
        Facet o1 = (Facet)arg0;
        Facet o2 = (Facet)arg1;
        return comparator.compare(o1, o2) > 0;
      }     
    };
    return queue;
  }

  public void close()
  {
    if (_closed)
    {
      return;
    }
    _closed = true;
    if (_list!=null)
    {
      for(FacetAccessible fa : _list)
      {
        fa.close();
      }
      _list.clear();
    }
  }

  public FacetIterator iterator()
  {
    if (_closed)
    {
      throw new IllegalStateException("This instance of count collector was already closed");
    }

    ArrayList<FacetIterator> iterList = new ArrayList<FacetIterator>(_list.size());
    FacetIterator iter;
    for (FacetAccessible facetAccessor : _list)
    {
      iter = (FacetIterator) facetAccessor.iterator();
      if(iter != null)
        iterList.add(iter);
    }
    if (iterList.get(0) instanceof IntFacetIterator)
    {
      ArrayList<IntFacetIterator> il = new ArrayList<IntFacetIterator>();
      for (FacetAccessible facetAccessor : _list)
      {
        iter = (FacetIterator) facetAccessor.iterator();
        if(iter != null)
          il.add((IntFacetIterator) iter);
      }
      return new CombinedIntFacetIterator(il, _fspec.getMinHitCount());
    }
    if (iterList.get(0) instanceof LongFacetIterator)
    {
      ArrayList<LongFacetIterator> il = new ArrayList<LongFacetIterator>();
      for (FacetAccessible facetAccessor : _list)
      {
        iter = (FacetIterator) facetAccessor.iterator();
        if(iter != null)
          il.add((LongFacetIterator) iter);
      }
      return new CombinedLongFacetIterator(il, _fspec.getMinHitCount());
    }
    if (iterList.get(0) instanceof ShortFacetIterator)
    {
      ArrayList<ShortFacetIterator> il = new ArrayList<ShortFacetIterator>();
      for (FacetAccessible facetAccessor : _list)
      {
        iter = (FacetIterator) facetAccessor.iterator();
        if(iter != null)
          il.add((ShortFacetIterator) iter);
      }
      return new CombinedShortFacetIterator(il, _fspec.getMinHitCount());
    }
    if (iterList.get(0) instanceof FloatFacetIterator)
    {
      ArrayList<FloatFacetIterator> il = new ArrayList<FloatFacetIterator>();
      for (FacetAccessible facetAccessor : _list)
      {
        iter = (FacetIterator) facetAccessor.iterator();
        if(iter != null)
          il.add((FloatFacetIterator) iter);
      }
      return new CombinedFloatFacetIterator(il, _fspec.getMinHitCount());
    }
    if (iterList.get(0) instanceof DoubleFacetIterator)
    {
      ArrayList<DoubleFacetIterator> il = new ArrayList<DoubleFacetIterator>();
      for (FacetAccessible facetAccessor : _list)
      {
        iter = (FacetIterator) facetAccessor.iterator();
        if(iter != null)
          il.add((DoubleFacetIterator) iter);
      }
      return new CombinedDoubleFacetIterator(il, _fspec.getMinHitCount());
    }
    return new CombinedFacetIterator(iterList);
  }
}
