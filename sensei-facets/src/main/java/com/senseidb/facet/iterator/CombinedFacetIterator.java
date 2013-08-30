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

import java.util.List;
import java.util.NoSuchElementException;

/**
 * @author nnarkhed
 *
 */
public class CombinedFacetIterator extends FacetIterator {
  
  private FacetIterator[] heap;
  private int size;
  List<FacetIterator> _iterators;
  
  public CombinedFacetIterator(final List<FacetIterator> iterators) {
    _iterators = iterators;
    heap = new FacetIterator[iterators.size() + 1];
    size = 0;
    for(FacetIterator iterator : iterators) {
      if(iterator.next(0) != null)
        add(iterator);
    }
    facet = null;
    count = 0;
  }
  
  private final void add(FacetIterator element) {
    size++;
    heap[size] = element;
    upHeap();
  }

  private final void upHeap() {
    int i = size;
    FacetIterator node = heap[i];            // save bottom node
    Comparable val = node.facet;
    int j = i >>> 1;
    while (j > 0 && val.compareTo(heap[j].facet) < 0) {
      heap[i] = heap[j];              // shift parents down
      i = j;
      j = j >>> 1;
    }
    heap[i] = node;               // install saved node
  }

  private final void downHeap() {
    int i = 1;
    FacetIterator node = heap[i];            // save top node
    Comparable val = node.facet;
    int j = i << 1;               // find smaller child
    int k = j + 1;
    if (k <= size && heap[k].facet.compareTo(heap[j].facet) < 0) {
      j = k;
    }
    while (j <= size && heap[j].facet.compareTo(val) < 0) {
      heap[i] = heap[j];              // shift up child
      i = j;
      j = i << 1;
      k = j + 1;
      if (k <= size && heap[k].facet.compareTo(heap[j].facet) < 0) {
        j = k;
      }
    }
    heap[i] = node;               // install saved node
  }
  
  private final void pop() {
    if (size > 0) {
      heap[1] = heap[size];           // move last to first
      heap[size] = null;              // permit GC of objects
      if(--size > 0) downHeap();                 // adjust heap
    }
  }

  /* (non-Javadoc)
   * @see com.browseengine.bobo.api.FacetIterator#next()
   */
  public Comparable next() {
    if(!hasNext())
      throw new NoSuchElementException("No more facets in this iteration");

    return next(1);
  }

  /**
   * This version of the next() method applies the minHits from the facet spec before returning the facet and its hitcount
   * @param minHits the minHits from the facet spec for CombinedFacetAccessible
   * @return        The next facet that obeys the minHits 
   */
  public Comparable next(int minHits) {
    if(size == 0)
    {
      facet = null;
      count = 0;
      return null;
    }

    FacetIterator node = heap[1];    
    facet = node.facet;
    count = node.count;
    int min = (minHits > 0 ? 1 : 0);
    while(true)
    {
      if(node.next(min) != null)
      {
        downHeap();
        node = heap[1];
      }
      else
      {
        pop();
        if(size > 0)
        {
          node = heap[1];
        }
        else
        {
          // we reached the end. check if this facet obeys the minHits
          if(count < minHits)
          {
            facet = null;
            count = 0;
          }
          break;
        }
      }
      Comparable next = node.facet;
      if (next==null) throw new RuntimeException();
      if(!next.equals(facet))
      {
        // check if this facet obeys the minHits
        if(count >= minHits)
          break;
        // else, continue iterating to the next facet
        facet = next;
        count = node.count;
      }
      else
      {
        count += node.count;
      }
    }
    return format(facet);
  }

  /* (non-Javadoc)
   * @see java.util.Iterator#hasNext()
   */
  public boolean hasNext() {
    return (size > 0);
  }

  /* (non-Javadoc)
   * @see java.util.Iterator#remove()
   */
  public void remove() {
    throw new UnsupportedOperationException("remove() method not supported for Facet Iterators");
  }

  @Override
  public String format(Object val)
  {
    return _iterators.get(0).format(val);
  }
}
