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

package com.senseidb.facet.data;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;

/**
 * A priority queue for int that uses int[] heap for storage.
 * Always have least element on top. When the queue is full, new elements are added
 * if they are bigger than the least element to replace them.
 * @author "Xiaoyang Gu<xgu@linkedin.com>"
 *
 */
public class IntBoundedPriorityQueue extends PriorityQueue<Integer>
{
  private final int             _capacity;
  private final int[]             _items;
  private int                   _size = 0;
  private IntComparator _comp;
  private final int _forbiddenValue;

  /**
   * @param capacity the maximum number of items the queue accepts
   * @param comparator a comparator that is used to order the items. 
   */
  @SuppressWarnings("unchecked")
  public IntBoundedPriorityQueue(IntComparator comparator, int capacity, int forbiddenValue)
  {
    _capacity = capacity;
    _comp = comparator;
    _items = new int[capacity];// java.lang.reflect.Array.newInstance(, capacity);
    _forbiddenValue = forbiddenValue;
  }

  /**
   * {@inheritDoc} Retrieves, but does not remove, the head of this queue. This
   * implementation returns the result of peek unless the queue is empty.
   * 
   * @see java.util.Queue#element()
   */
  public Integer element() throws NoSuchElementException
  {
    if (_size == 0)
      throw new NoSuchElementException("empty queue");
    return _items[0];
  }

  public int intElement() throws NoSuchElementException
  {
    if (_size == 0)
      throw new NoSuchElementException("empty queue");
    return _items[0];
  }

  /**
   * Returns an iterator over the elements in this collection. There are no guarantees
   * concerning the order in which the elements are returned (unless this collection is an
   * instance of some class that provides a guarantee).
   * 
   * @see java.util.AbstractCollection#iterator()
   */
  @Override
  public IntIterator iterator()
  {
    return new IntIterator()
    {
      private int i = 0;

      public boolean hasNext()
      {
        return i < _size;
      }

      public Integer next() throws NoSuchElementException
      {
        if (i >= _size)
          throw new NoSuchElementException("last element reached in queue");
        return _items[i++];
      }

      public int nextInt() throws NoSuchElementException
      {
        if (i >= _size)
          throw new NoSuchElementException("last element reached in queue");
        return _items[i++];
      }

      public void remove()
      {
        throw new UnsupportedOperationException("not supported");
      }

    };
  }
  
  public interface IntIterator extends Iterator<Integer>
  {
    public int nextInt();
  }

  /**
   * When the queue is full, the offered elements are added if they are bigger than the
   * smallest one already in the queue.
   * <br>
   * Inserts the specified element into this queue, if possible. When using queues that
   * may impose insertion restrictions (for example capacity bounds), method offer is
   * generally preferable to method Collection.add, which can fail to insert an element
   * only by throwing an exception.
   * 
   * @see java.util.Queue#offer(Object)
   */
  public boolean offer(Integer item)
  {
    int itm = item;
    return offer(itm);
  }

  /**
   * When the queue is full, the offered elements are added if they are bigger than the
   * smallest one already in the queue.
   * <br>
   * Inserts the specified element into this queue, if possible. When using queues that
   * may impose insertion restrictions (for example capacity bounds), method offer is
   * generally preferable to method Collection.add, which can fail to insert an element
   * only by throwing an exception.
   *
   * @see java.util.Queue#offer(Object)
   */
  public boolean offer(int item)
  {
    if (_size < _capacity)
    {
      _items[_size] = item;
      percolateUp(_size);
      _size++;
      //    System.out.println("adding  to queue " + item + "  \t  " +Thread.currentThread().getClass()+Thread.currentThread().getId() );
      return true;
    } else
    {
      if (_items[0]<item)
      {
        _items[0] = item;
        percolateDown();
        return true;
      } else
        return false;
    }
  }

  /**
   * Retrieves, but does not remove, the head of this queue, returning null if this queue
   * is empty.
   * 
   * @see java.util.Queue#peek()
   */
  public Integer peek()
  {
    if (_size == 0)
      return null;
    return _items[0];
  }

  /**
   * Retrieves, but does not remove, the head of this queue, returning the <b>forbidden value</b>
   * if the queue is empty.
   * @return 
   */
  public int peekInt()
  {
    if (_size == 0)
      return _forbiddenValue;
    return _items[0];
  }

  /**
   * Retrieves and removes the head of this queue, or null if this queue is empty.
   * 
   * @see java.util.Queue#poll()
   */
  public Integer poll()
  {
    if (_size == 0)
      return null;
    int ret = _items[0];
    _size--;
    _items[0] = _items[_size];
    _items[_size] = 0;
    if (_size > 1)
      percolateDown();
    return ret;
  }

  /**
   * Retrieves and removes the head of this queue, or the <b>forbidden value</b> if this queue is empty.
   * @return
   */
  public int pollInt()
  {
    if (_size == 0)
      return _forbiddenValue;
    int ret = _items[0];
    _size--;
    _items[0] = _items[_size];
    _items[_size] = 0;
    if (_size > 1)
      percolateDown();
    return ret;
  }

  /**
   * Returns the number of elements in this collection.
   * 
   * @see java.util.AbstractCollection#size()
   */
  @Override
  public int size()
  {
    return _size;
  }

  private void percolateDown()
  {
    int temp = _items[0];
    int index = 0;
    while (true)
    {
      int left = (index << 1) + 1;

      int right = left + 1;
      if (right < _size)
      {
        left = _comp.compare(_items[left], _items[right]) < 0 ? left : right;
      }
      else if (left >= _size)
      {
        _items[index] = temp;
        break;
      }
      if (_comp.compare(_items[left], temp) < 0)
      {
        _items[index] = _items[left];
        index = left;
      }
      else
      {
        _items[index] = temp;
        break;
      }
    }
  }

  private void percolateUp(int index)
  {
    int i;
    int temp = _items[index];
    while ((i = ((index - 1) >> 1)) >= 0 && _comp.compare(temp, _items[i]) < 0)
    {
      _items[index] = _items[i];
      index = i;
    }
    _items[index] = temp;
  }
}
