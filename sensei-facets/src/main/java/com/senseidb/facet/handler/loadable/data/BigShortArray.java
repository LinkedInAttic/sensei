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


import com.senseidb.facet.handler.loadable.data.BigSegmentedArray;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.OpenBitSet;

import java.util.Arrays;

public class BigShortArray extends BigSegmentedArray {
	 private static final long serialVersionUID = 1L;
		
	  private short[][] _array;
	  
	  /* Remember that 2^SHIFT_SIZE = BLOCK_SIZE */
	  final private static int BLOCK_SIZE = 2048;
	  final private static int SHIFT_SIZE = 11; 
	  final private static int MASK = BLOCK_SIZE -1;
	  
	  public BigShortArray(int size)
	  {
	    super(size);
	    _array = new short[_numrows][];
	    for (int i = 0; i < _numrows; i++)
	    {
	      _array[i]=new short[BLOCK_SIZE];
	    }
	  }

	  @Override
	  public final void add(int docId, int val)
	  {
	    _array[docId >> SHIFT_SIZE][docId & MASK] = (short)val;
	  }

	  @Override
	  public final int get(int docId)
	  {
	    return _array[docId >> SHIFT_SIZE][docId & MASK];
	  }
	  
	  @Override
	  public final int findValue(int val, int docId, int maxId)
	  {
	    while(true)
	    {
	      if(_array[docId >> SHIFT_SIZE][docId & MASK] == val) return docId;
	      if(docId++ >= maxId) break;
	    }
	    return DocIdSetIterator.NO_MORE_DOCS;
	  }
	  
	  @Override
	  public final int findValues(OpenBitSet bitset, int docId, int maxId)
	  {
	    while(true)
	    {
	      if(bitset.fastGet(_array[docId >> SHIFT_SIZE][docId & MASK])) return docId;
	      if(docId++ >= maxId) break;
	    }
	    return DocIdSetIterator.NO_MORE_DOCS;
	  }

	  @Override
	  public final int findValueRange(int minVal, int maxVal, int docId, int maxId)
	  {
	    while(true)
	    {
	      int val = _array[docId >> SHIFT_SIZE][docId & MASK];
	      if(val >= minVal && val <= maxVal) return docId;
	      if(docId++ >= maxId) break;
	    }
	    return DocIdSetIterator.NO_MORE_DOCS;
	  }
	  
	  @Override
	  public final int findBits(int bits, int docId, int maxId)
	  {
	    while(true)
	    {
	      if((_array[docId >> SHIFT_SIZE][docId & MASK] & bits) != 0) return docId;
	      if(docId++ >= maxId) break;
	    }
	    return DocIdSetIterator.NO_MORE_DOCS;
	  }

	  @Override
	  public final void fill(int val)
	  {
		short shortVal = (short)val;
	    for(short[] block : _array)
	    {
	      Arrays.fill(block, shortVal);
	    }
	  }

	  @Override
	  public void ensureCapacity(int size)
	  {
	    int newNumrows = (size >> SHIFT_SIZE) + 1;
	    if (newNumrows > _array.length)
	    {
	      short[][] newArray = new short[newNumrows][];           // grow
	      System.arraycopy(_array, 0, newArray, 0, _array.length);
	      for (int i = _array.length; i < newNumrows; ++i)
	      {
	        newArray[i] = new short[BLOCK_SIZE];
	      }
	      _array = newArray;
	    }
	    _numrows = newNumrows;
	  }

	  @Override
	  final int getBlockSize() {
		return BLOCK_SIZE;
	  }

	  @Override
	  final int getShiftSize() {
		return SHIFT_SIZE;
	  }

	  @Override
	  public int maxValue() {
		return Short.MAX_VALUE;
	  }
}
