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

import java.util.List;
import java.util.NoSuchElementException;

/**
 * @author nnarkhed
 *
 */
public class DefaultCombinedFacetIterator extends CombinedFacetIterator {
  
  private FacetIterator[] _heap;
  private int _size;
  private List<FacetIterator> _iterators;
  
  public DefaultCombinedFacetIterator(final List<FacetIterator> iterators) {
    _iterators = iterators;
    _heap = new FacetIterator[iterators.size() + 1];
    _size = 0;
    for(FacetIterator iterator : iterators) {
      if(iterator.next() != null)
        add(iterator);
    }
    _facet = null;
    _count = 0;
  }
  
  private final void add(FacetIterator element) {
    _size++;
    _heap[_size] = element;
    upHeap();
  }

  private final void upHeap() {
    int i = _size;
    FacetIterator node = _heap[i];            // save bottom node
    Comparable val = node._facet;
    int j = i >>> 1;
    while (j > 0 && val.compareTo(_heap[j]._facet) < 0) {
      _heap[i] = _heap[j];              // shift parents down
      i = j;
      j = j >>> 1;
    }
    _heap[i] = node;               // install saved node
  }

  private final void downHeap() {
    int i = 1;
    FacetIterator node = _heap[i];            // save top node
    Comparable val = node._facet;
    int j = i << 1;               // find smaller child
    int k = j + 1;
    if (k <= _size && _heap[k]._facet.compareTo(_heap[j]._facet) < 0) {
      j = k;
    }
    while (j <= _size && _heap[j]._facet.compareTo(val) < 0) {
      _heap[i] = _heap[j];              // shift up child
      i = j;
      j = i << 1;
      k = j + 1;
      if (k <= _size && _heap[k]._facet.compareTo(_heap[j]._facet) < 0) {
        j = k;
      }
    }
    _heap[i] = node;               // install saved node
  }
  
  private final void pop() {
    if (_size > 0) {
      _heap[1] = _heap[_size];           // move last to first
      _heap[_size] = null;              // permit GC of objects
      if(--_size > 0) downHeap();                 // adjust _heap
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
   * This version of the next() method applies the minHits from the _facet spec before returning the _facet and its hitcount
   * @param minHits the minHits from the _facet spec for CombinedFacetAccessible
   * @return        The next _facet that obeys the minHits
   */
  public Comparable next(int minHits) {
    if(_size == 0)
    {
      _facet = null;
      _count = 0;
      return null;
    }

    FacetIterator node = _heap[1];
    _facet = node._facet;
    _count = node._count;
    while(true)
    {
      if(node.next() != null)
      {
        downHeap();
        node = _heap[1];
      }
      else
      {
        pop();
        if(_size > 0)
        {
          node = _heap[1];
        }
        else
        {
          // we reached the end. check if this _facet obeys the minHits
          if(_count < minHits)
          {
            _facet = null;
            _count = 0;
          }
          break;
        }
      }
      Comparable next = node._facet;
      if (next==null) throw new RuntimeException();
      if(!next.equals(_facet))
      {
        // check if this _facet obeys the minHits
        if(_count >= minHits)
          break;
        // else, continue iterating to the next _facet
        _facet = next;
        _count = node._count;
      }
      else
      {
        _count += node._count;
      }
    }
    return _facet;
  }

  /* (non-Javadoc)
   * @see java.util.Iterator#hasNext()
   */
  public boolean hasNext() {
    return (_size > 0);
  }
}
