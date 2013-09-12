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

/**
 * @author "Xiaoyang Gu<xgu@linkedin.com>"
 *
 */
public class CombinedNumericFacetIterator extends CombinedFacetIterator
{
  private static class LongIteratorNode
  {
    public NumericFacetIterator _iterator;
    public long _curFacet;
    public int _curFacetCount;

    public LongIteratorNode(NumericFacetIterator iterator)
    {
      _iterator = iterator;
      _curFacet = NumericFacetIterator.VALUE_MISSING;
      _curFacetCount = 0;
    }

    public boolean fetch()
    {
      if( (_curFacet = _iterator.nextLong()) != NumericFacetIterator.VALUE_MISSING)
      {
        _curFacetCount = _iterator._count;
        return true;
      }
      _curFacet = NumericFacetIterator.VALUE_MISSING;
      _curFacetCount = 0;
      return false;
    }
  }

  private final LongFacetPriorityQueue _queue;

  private List<NumericFacetIterator> _iterators;

  private long _longFacet;

  private CombinedNumericFacetIterator(final int length) {
    _queue = new LongFacetPriorityQueue();
    _queue.initialize(length);
  }

  public CombinedNumericFacetIterator(final List<NumericFacetIterator> iterators) {
    this(iterators.size());
    _iterators = iterators;
    for(NumericFacetIterator iterator : iterators) {
      LongIteratorNode node = new LongIteratorNode(iterator);
      if(node.fetch())
        _queue.add(node);
    }
    _longFacet = NumericFacetIterator.VALUE_MISSING;
    _count = 0;
  }

  /* (non-Javadoc)
   * @see java.util.Iterator#hasNext()
   */
  public boolean hasNext() {
    return (_queue.size() > 0);
  }

  /**
   * Lucene PriorityQueue
   *
   */
  public static class LongFacetPriorityQueue
  {
    private int size;
    private int maxSize;
    protected LongIteratorNode[] heap;

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
      heap = new LongIteratorNode[heapSize];
      this.maxSize = maxSize;
    }

    public final void put(LongIteratorNode element)
    {
      size++;
      heap[size] = element;
      upHeap();
    }

    public final LongIteratorNode add(LongIteratorNode element)
    {
      size++;
      heap[size] = element;
      upHeap();
      return heap[1];
    }

    public boolean insert(LongIteratorNode element)
    {
      return insertWithOverflow(element) != element;
    }

    public LongIteratorNode insertWithOverflow(LongIteratorNode element)
    {
      if (size < maxSize)
      {
        put(element);
        return null;
      } else if (size > 0 && !(element._curFacet < heap[1]._curFacet))
      {
        LongIteratorNode ret = heap[1];
        heap[1] = element;
        adjustTop();
        return ret;
      } else
      {
        return element;
      }
    }

    /** Returns the least element of the PriorityQueue in constant time. */
    public final LongIteratorNode top()
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
    public final LongIteratorNode pop()
    {
      if (size > 0)
      {
        LongIteratorNode result = heap[1]; // save first value
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

    public final LongIteratorNode updateTop()
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
      LongIteratorNode node = heap[i]; // save bottom node
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
      LongIteratorNode node = heap[i]; // save top node
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

  private long nextLong()
  {
    int qsize = _queue.size();
    if(qsize == 0)
    {
      _longFacet = NumericFacetIterator.VALUE_MISSING;
      _count = 0;
      return NumericFacetIterator.VALUE_MISSING;
    }

    LongIteratorNode node = _queue.top();
    _longFacet = node._curFacet;
    _count = node._curFacetCount;
    while(true)
    {
      if(node.fetch())
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
          // we reached the end
          break;
        }
      }
      long next = node._curFacet;
      if(next!=_longFacet)
      {
        break;
      }
      else
      {
        _count += node._curFacetCount;
      }
    }
    return _longFacet;
  }

  public Long next(int minHits)
  {
    long facet;
    while ((facet = nextLong()) != NumericFacetIterator.VALUE_MISSING)
    {
      if (_count >= minHits)
        return facet;
    }
    return null;
  }
}
