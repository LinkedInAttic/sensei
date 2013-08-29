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

import java.util.List;
import java.util.NoSuchElementException;

public class CombinedIntFacetIterator extends IntFacetIterator
{

  public int facet;

  private static class IntIteratorNode
  {
    public IntFacetIterator _iterator;
    public int _curFacet;
    public int _curFacetCount;

    public IntIteratorNode(IntFacetIterator iterator)
    {
      _iterator = iterator;
      _curFacet = TermIntList.VALUE_MISSING;
      _curFacetCount = 0;
    }

    public boolean fetch(int minHits)
    {
      if(minHits > 0)
        minHits = 1;
      if( (_curFacet = _iterator.nextInt(minHits)) != TermIntList.VALUE_MISSING)
      {
        _curFacetCount = _iterator.count;
        return true;
      }
      _curFacet = TermIntList.VALUE_MISSING;
      _curFacetCount = 0;
      return false;
    }

    public String peek()//bad
    {
      throw new UnsupportedOperationException();
//      if(_iterator.hasNext()) 
//      {
//        return _iterator.getFacet();
//      }
//      return null;
    }
  }

  private final IntFacetPriorityQueue _queue;

  private List<IntFacetIterator> _iterators;

  private CombinedIntFacetIterator(final int length) {
    _queue = new IntFacetPriorityQueue();
    _queue.initialize(length);   
  }

  public CombinedIntFacetIterator(final List<IntFacetIterator> iterators) {
    this(iterators.size());
    _iterators = iterators;
    for(IntFacetIterator iterator : iterators) {
      IntIteratorNode node = new IntIteratorNode(iterator);
      if(node.fetch(1))
        _queue.add(node);
    }
    facet = TermIntList.VALUE_MISSING;
    count = 0;
  }

  public CombinedIntFacetIterator(final List<IntFacetIterator> iterators, int minHits) {
    this(iterators.size());
    _iterators = iterators;
    for(IntFacetIterator iterator : iterators) {
      IntIteratorNode node = new IntIteratorNode(iterator);
      if(node.fetch(minHits))
        _queue.add(node);
    }
    facet = TermIntList.VALUE_MISSING;
    count = 0;
  }

  /* (non-Javadoc)
   * @see com.browseengine.bobo.api.FacetIterator#getFacet()
   */
  public String getFacet() {
    if (facet == TermIntList.VALUE_MISSING) return null;
    return format(facet);
  }
  public String format(int val)
  {
    return _iterators.get(0).format(val);
  }
  public String format(Object val)
  {
    return _iterators.get(0).format(val);
  }
  /* (non-Javadoc)
   * @see com.browseengine.bobo.api.FacetIterator#getFacetCount()
   */
  public int getFacetCount() {
    return count;
  }

  /* (non-Javadoc)
   * @see com.browseengine.bobo.api.FacetIterator#next()
   */
  public String next() {
    if(!hasNext())
      throw new NoSuchElementException("No more facets in this iteration");

    IntIteratorNode node = _queue.top();

    facet = node._curFacet;
    int next = TermIntList.VALUE_MISSING;
    count = 0;
    while(hasNext())
    {
      node = _queue.top();
      next = node._curFacet;
      if( (next != TermIntList.VALUE_MISSING) && (next!=facet) )
      {
        return format(facet);
      }
      count += node._curFacetCount;
      if(node.fetch(1))
        _queue.updateTop();
      else
        _queue.pop();
    }
    return null;
  }

  /**
   * This version of the next() method applies the minHits from the facet spec before returning the facet and its hitcount
   * @param minHits the minHits from the facet spec for CombinedFacetAccessible
   * @return        The next facet that obeys the minHits 
   */
  public String next(int minHits) {
    int qsize = _queue.size();
    if(qsize == 0)
    {
      facet = TermIntList.VALUE_MISSING;
      count = 0;
      return null;
    }

    IntIteratorNode node = _queue.top();    
    facet = node._curFacet;
    count = node._curFacetCount;
    while(true)
    {
      if(node.fetch(minHits))
      {
        node = _queue.updateTop();
      }
      else
      {
        _queue.pop();
        if(--qsize > 0)
        {
          node = _queue.top();
        }
        else
        {
          // we reached the end. check if this facet obeys the minHits
          if(count < minHits)
          {
            facet = TermIntList.VALUE_MISSING;
            count = 0;
            return null;
          }
          break;
        }
      }
      int next = node._curFacet;
      if(next!=facet)
      {
        // check if this facet obeys the minHits
        if(count >= minHits)
          break;
        // else, continue iterating to the next facet
        facet = next;
        count = node._curFacetCount;
      }
      else
      {
        count += node._curFacetCount;
      }
    }
    return format(facet);
  }

  /* (non-Javadoc)
   * @see java.util.Iterator#hasNext()
   */
  public boolean hasNext() {
    return (_queue.size() > 0);
  }

  /* (non-Javadoc)
   * @see java.util.Iterator#remove()
   */
  public void remove() {
    throw new UnsupportedOperationException("remove() method not supported for Facet Iterators");
  }

  /**
   * Lucene PriorityQueue
   * 
   */
  public static class IntFacetPriorityQueue
  {
    private int size;
    private int maxSize;
    protected IntIteratorNode[] heap;

    /** Subclass constructors must call this. */
    protected final void initialize(int maxSize)
    {
      size = 0;
      int heapSize;
      if (0 == maxSize)
        // We allocate 1 extra to avoid if statement in top()
        heapSize = 2;
      else
        heapSize = maxSize + 1;
      heap = new IntIteratorNode[heapSize];
      this.maxSize = maxSize;
    }

    public final void put(IntIteratorNode element)
    {
      size++;
      heap[size] = element;
      upHeap();
    }

    public final IntIteratorNode add(IntIteratorNode element)
    {
      size++;
      heap[size] = element;
      upHeap();
      return heap[1];
    }

    public boolean insert(IntIteratorNode element)
    {
      return insertWithOverflow(element) != element;
    }

    public IntIteratorNode insertWithOverflow(IntIteratorNode element)
    {
      if (size < maxSize)
      {
        put(element);
        return null;
      } else if (size > 0 && !(element._curFacet < heap[1]._curFacet))
      {
        IntIteratorNode ret = heap[1];
        heap[1] = element;
        adjustTop();
        return ret;
      } else
      {
        return element;
      }
    }

    /** Returns the least element of the PriorityQueue in constant time. */
    public final IntIteratorNode top()
    {
      // We don't need to check size here: if maxSize is 0,
      // then heap is length 2 array with both entries null.
      // If size is 0 then heap[1] is already null.
      return heap[1];
    }

    /**
     * Removes and returns the least element of the PriorityQueue in log(size)
     * time.
     */
    public final IntIteratorNode pop()
    {
      if (size > 0)
      {
        IntIteratorNode result = heap[1]; // save first value
        heap[1] = heap[size]; // move last to first
        heap[size] = null; // permit GC of objects
        size--;
        downHeap(); // adjust heap
        return result;
      } else
        return null;
    }

    public final void adjustTop()
    {
      downHeap();
    }

    public final IntIteratorNode updateTop()
    {
      downHeap();
      return heap[1];
    }

    /** Returns the number of elements currently stored in the PriorityQueue. */
    public final int size()
    {
      return size;
    }

    /** Removes all entries from the PriorityQueue. */
    public final void clear()
    {
      for (int i = 0; i <= size; i++)
      {
        heap[i] = null;
      }
      size = 0;
    }

    private final void upHeap()
    {
      int i = size;
      IntIteratorNode node = heap[i]; // save bottom node
      int j = i >>> 1;
      while (j > 0 && (node._curFacet < heap[j]._curFacet))
      {
        heap[i] = heap[j]; // shift parents down
        i = j;
        j = j >>> 1;
      }
      heap[i] = node; // install saved node
    }

    private final void downHeap()
    {
      int i = 1;
      IntIteratorNode node = heap[i]; // save top node
      int j = i << 1; // find smaller child
      int k = j + 1;
      if (k <= size && (heap[k]._curFacet < heap[j]._curFacet))
      {
        j = k;
      }
      while (j <= size && (heap[j]._curFacet < node._curFacet))
      {
        heap[i] = heap[j]; // shift up child
        i = j;
        j = i << 1;
        k = j + 1;
        if (k <= size && (heap[k]._curFacet < heap[j]._curFacet))
        {
          j = k;
        }
      }
      heap[i] = node; // install saved node
    }
  }

  /** (non-Javadoc)
   * @see IntFacetIterator#nextInt()
   */
  @Override
  public int nextInt()
  {
    if(!hasNext())
      throw new NoSuchElementException("No more facets in this iteration");

    IntIteratorNode node = _queue.top();

    facet = node._curFacet;
    int next = TermIntList.VALUE_MISSING;
    count = 0;
    while(hasNext())
    {
      node = _queue.top();
      next = node._curFacet;
      if( (next != TermIntList.VALUE_MISSING) && (next!=facet) )
      {
        return facet;
      }
      count += node._curFacetCount;
      if(node.fetch(1))
        _queue.updateTop();
      else
        _queue.pop();
    }
    return TermIntList.VALUE_MISSING;
  }

  @Override
  public int nextInt(int minHits)
  {
    int qsize = _queue.size();
    if(qsize == 0)
    {
      facet = TermIntList.VALUE_MISSING;
      count = 0;
      return TermIntList.VALUE_MISSING;
    }

    IntIteratorNode node = _queue.top();    
    facet = node._curFacet;
    count = node._curFacetCount;
    while(true)
    {
      if(node.fetch(minHits))
      {
        node = _queue.updateTop();
      }
      else
      {
        _queue.pop();
        if(--qsize > 0)
        {
          node = _queue.top();
        }
        else
        {
          // we reached the end. check if this facet obeys the minHits
          if(count < minHits)
          {
            facet = TermIntList.VALUE_MISSING;
            count = 0;
          }
          break;
        }
      }
      int next = node._curFacet;
      if(next!=facet)
      {
        // check if this facet obeys the minHits
        if(count >= minHits)
          break;
        // else, continue iterating to the next facet
        facet = next;
        count = node._curFacetCount;
      }
      else
      {
        count += node._curFacetCount;
      }
    }
    return facet;
  }
}
