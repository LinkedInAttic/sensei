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

import org.apache.lucene.util.OpenBitSet;

/**
 * Breaks up a regular java array by splitting it into a 2 dimensional array with
 * a predefined block size. Attempts to induce more efficient GC.  
 */

public abstract class BigSegmentedArray {

	protected final int _size;
	protected final int _blockSize;
	protected final int _shiftSize;

	protected int _numrows;
	  
	public BigSegmentedArray(int size)
	{
	  _size = size;
	  _blockSize = getBlockSize();
	  _shiftSize = getShiftSize();
	  _numrows = (size >> _shiftSize) + 1;
	}
	
	public int size(){
	  return _size;
	}
	
	abstract int getBlockSize();
	
	// TODO: maybe this should be automatically calculated
	abstract int getShiftSize();
	
	abstract public int get(int id);
	
	public int capacity(){
	  return _numrows * _blockSize;
	}
	
	abstract public void add(int id, int val);
	
	abstract public void fill(int val);
	  
	abstract public void ensureCapacity(int size);
	
	abstract public int maxValue();
	
	abstract public int findValue(int val, int id, int maxId);
	
	abstract public int findValues(OpenBitSet bitset, int id, int maxId);

	abstract public int findValueRange(int minVal, int maxVal, int id, int maxId);
	  
	abstract public int findBits(int bits, int id, int maxId);
}
