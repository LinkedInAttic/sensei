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

package com.senseidb.facet.handler.loadable.data;


import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.OpenBitSet;

import java.io.Serializable;
import java.util.Arrays;

/**
 * @author jko
 * 
 * BigSegmentedArray that creates segments only when the corresponding index is
 * being accessed.
 */
public class LazyBigIntArray extends BigSegmentedArray implements Serializable
{
  private static final long serialVersionUID = 1L;

  private int[][] _array;
  /* Remember that 2^SHIFT_SIZE = BLOCK_SIZE */
  final private static int BLOCK_SIZE = 1024;
  final private static int SHIFT_SIZE = 10; 
  final private static int MASK = BLOCK_SIZE -1;

  private int _fillValue = 0;
 
  public LazyBigIntArray(int size)
  {
    super(size);
    // initialize empty blocks
    _array = new int[_numrows][];
  }

  /* (non-Javadoc)
   * @see com.browseengine.bobo.util.BigSegmentedArray#getBlockSize()
   */
  @Override
  int getBlockSize()
  {
    return BLOCK_SIZE;
  }

  /* (non-Javadoc)
   * @see com.browseengine.bobo.util.BigSegmentedArray#getShiftSize()
   */
  @Override
  int getShiftSize()
  {
    return SHIFT_SIZE;
  }

  /* (non-Javadoc)
   * @see com.browseengine.bobo.util.BigSegmentedArray#get(int)
   */
  @Override
  public int get(int id)
  {
    int i = id >> SHIFT_SIZE;
    if (_array[i] == null)
      return _fillValue; // return _fillValue to mimic int[] behavior
    else
      return _array[i][id & MASK];
  }

  /* (non-Javadoc)
   * @see com.browseengine.bobo.util.BigSegmentedArray#add(int, int)
   */
  @Override
  public void add(int id, int val)
  {
    int i = id >> SHIFT_SIZE;
    if (_array[i] == null)
    {
      _array[i] = new int[BLOCK_SIZE];
      if (_fillValue != 0)
        Arrays.fill(_array[i], _fillValue);
    }
    _array[i][id & MASK] = val;
  }

  /* (non-Javadoc)
   * @see com.browseengine.bobo.util.BigSegmentedArray#fill(int)
   */
  @Override
  public void fill(int val)
  {
    for(int[] block : _array)
    {
      if (block == null) continue;
      Arrays.fill(block, val);
    }

    _fillValue = val;
  }

  /* (non-Javadoc)
   * @see com.browseengine.bobo.util.BigSegmentedArray#ensureCapacity(int)
   */
  @Override
  public void ensureCapacity(int size)
  {
    int newNumrows = (size >> SHIFT_SIZE) + 1;
    if (newNumrows > _array.length)
    {
      int[][] newArray = new int[newNumrows][];           // grow
      System.arraycopy(_array, 0, newArray, 0, _array.length);
      // don't allocate new rows
      _array = newArray;
    }
    _numrows = newNumrows;
  }

  /* (non-Javadoc)
   * @see com.browseengine.bobo.util.BigSegmentedArray#maxValue()
   */
  @Override
  public int maxValue()
  {
    return Integer.MAX_VALUE;
  }

  /* (non-Javadoc)
   * @see com.browseengine.bobo.util.BigSegmentedArray#findValue(int, int, int)
   */
  
  @Override
  public int findValue(int val, int id, int maxId)
  {
    while (id <= maxId)
    {
      int i = id >> SHIFT_SIZE;
      if (_array[i] == null)
      {
        if (val == _fillValue)
          return id;
        else
          id = (i + 1) << SHIFT_SIZE; // jump to next segment
      }
      else
      {
        if (_array[i][id & MASK] == val) 
          return id;
        else
          id++;
      }
    }
    return DocIdSetIterator.NO_MORE_DOCS;
  }
  
  /* (non-Javadoc)
   * @see com.browseengine.bobo.util.BigSegmentedArray#findValues(org.apache.lucene.util.OpenBitSet, int, int)
   */
  @Override
  public int findValues(OpenBitSet bitset, int id, int maxId)
  {
    while (id <= maxId)
    {
      int i = id >> SHIFT_SIZE;
      if (_array[i] == null)
      {
        if (bitset.fastGet(_fillValue))
          return id;
        else
          id = (i + 1) << SHIFT_SIZE; // jump to next segment
      }
      else
      {
        if (bitset.fastGet(_array[i][id & MASK])) 
          return id;
        else
          id++;
      }
    }
    return DocIdSetIterator.NO_MORE_DOCS;
  }

  /* (non-Javadoc)
   * @see com.browseengine.bobo.util.BigSegmentedArray#findValueRange(int, int, int, int)
   */
  @Override
  public int findValueRange(int minVal, int maxVal, int id, int maxId)
  {
    while (id <= maxId)
    {
      int i = id >> SHIFT_SIZE;
      if (_array[i] == null)
      {
        if (_fillValue >= minVal && _fillValue <= maxVal)
          return id;
        else
          id = (i + 1) << SHIFT_SIZE; // jump to next segment
      }
      else
      {
        int val = _array[i][id & MASK];
        if (val >= minVal && val <= maxVal)
          return id;
        else
          id++;
      }
    }
    return DocIdSetIterator.NO_MORE_DOCS;
  }

  /* (non-Javadoc)
   * @see com.browseengine.bobo.util.BigSegmentedArray#findBits(int, int, int)
   */
  @Override
  public int findBits(int bits, int id, int maxId)
  {
    while (id <= maxId)
    {
      int i = id >> SHIFT_SIZE;
      if (_array[i] == null)
      {
        if ((_fillValue & bits) != 0)
          return id;
        else
          id = (i + 1) << SHIFT_SIZE; // jump to next segment
      }
      else
      {
        int val = _array[i][id & MASK];
        if ((val & bits) != 0)
          return id;
        else
          id++;
      }
    }
    return DocIdSetIterator.NO_MORE_DOCS;
  }
}
