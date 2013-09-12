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

package com.senseidb.facet.iterator.loadable;

import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.List;

public class TermIntList extends TermNumberList<Integer>
{
  private int[] _elements = null;
  private boolean withDummy = true;
  public static final int VALUE_MISSING = Integer.MIN_VALUE;
  private static int parse(String s)
  {
    if (s == null || s.length() == 0)
    {
      return 0;
    } else
    {
      return Integer.parseInt(s);
    }
  }

  public TermIntList()
  {
    super();
  }

  public TermIntList(int capacity, String formatString)
  {
    super(capacity, formatString);
  }

  public TermIntList(String formatString)
  {
    super(formatString);
  }

  @Override
  public boolean add(String o)
  {
    if (_innerList.size() == 0 && o!=null) withDummy = false; // the first value added is not null
    int item = parse(o);
  
    return ((IntArrayList) _innerList).add(item);
  }

	@Override
	public boolean addRaw(Object o)
  {
		return ((IntArrayList) _innerList).add(((Number)o).intValue());
	}

  @Override
  protected List<?> buildPrimitiveList(int capacity)
  {
    _type = Integer.class;
    return capacity > 0 ? new IntArrayList(capacity) : new IntArrayList();
  }

  @Override
  public void clear()
  {
    super.clear();
  }

  @Override
  public String get(int index)
  {
    DecimalFormat formatter = _formatter.get();
    if (formatter == null)
      return String.valueOf(_elements[index]);
    return formatter.format(_elements[index]);
  }

  public int getPrimitiveValue(int index)
  {
    if (index < _elements.length)
      return _elements[index];
    else
      return TermIntList.VALUE_MISSING;
  }

  @Override
  public int indexOf(Object o)
  {
    if (withDummy)
    {
      if (o==null) return -1;
      int val;
      if (o instanceof String)
        val = parse((String) o);
      else
        val = (Integer)o;
      return Arrays.binarySearch(_elements, 1, _elements.length, val);
    } else
    {
      int val;
      if (o instanceof String)
        val = parse((String) o);
      else
        val = (Integer)o;
      return Arrays.binarySearch(_elements, val);
    }
  }

  public int indexOf(Integer value)
  {
    if (withDummy)
    {
      if (value==null) return -1;
      return Arrays.binarySearch(_elements, 1, _elements.length, value.intValue());
    } else
    {
      return Arrays.binarySearch(_elements, value.intValue());
    }
  }

  public int indexOf(int val)
  {
    if (withDummy)
      return Arrays.binarySearch(_elements, 1, _elements.length, val);
    else
      return Arrays.binarySearch(_elements, val);
  }

  @Override
  public int indexOfWithOffset(Object value, int offset)
  {
    if (withDummy)
    {
      if (value == null || offset >= _elements.length)
        return -1;
      int val = parse(String.valueOf(value));
      return Arrays.binarySearch(_elements, offset, _elements.length, val);
    }
    else
    {
      int val = parse(String.valueOf(value));
      return Arrays.binarySearch(_elements, offset, _elements.length, val);
    }
  }

  public int indexOfWithOffset(Integer value, int offset)
  {
    if (withDummy)
    {
      if (value==null || offset >= _elements.length)
        return -1;
      return Arrays.binarySearch(_elements, offset, _elements.length, value.intValue());
    }
    else
    {
      return Arrays.binarySearch(_elements, offset, _elements.length, value.intValue());
    }
  }

  public int indexOfWithOffset(int value, int offset)
  {
    if (withDummy)
    {
      if (offset >= _elements.length)
        return -1;
      return Arrays.binarySearch(_elements, offset, _elements.length, value);
    }
    else
    {
      return Arrays.binarySearch(_elements, offset, _elements.length, value);
    }
  }

  @Override
  public int indexOfWithType(Integer val)
  {
    if (withDummy)
    {
      if (val == null) return -1;
      return Arrays.binarySearch(_elements, 1, _elements.length, val.intValue());
    } else
    {
      return Arrays.binarySearch(_elements, val.intValue());
    }
  }

  public int indexOfWithType(int val)
  {
    if (withDummy)
    {
      return Arrays.binarySearch(_elements, 1, _elements.length, val);
    } else
    {
      return Arrays.binarySearch(_elements, val);
    }
  }

  @Override
  public void seal()
  {
    ((IntArrayList) _innerList).trim();
    _elements = ((IntArrayList) _innerList).elements();
    int negativeIndexCheck = withDummy ? 1 : 0;
    //reverse negative elements, because string order and numeric orders are completely opposite
    if (_elements.length > negativeIndexCheck && _elements[negativeIndexCheck] < 0) {
      int endPosition = indexOfWithType(0);
      if (endPosition < 0) {
        endPosition = -1 *endPosition - 1;
      }
      int tmp;
      for (int i = 0;  i < (endPosition - negativeIndexCheck) / 2; i++) {
         tmp = _elements[i + negativeIndexCheck];
         _elements[i + negativeIndexCheck] = _elements[endPosition -i -1];
         _elements[endPosition -i -1] = tmp;
      }
    }
  }

  @Override
  protected Object parseString(String o)
  {
    return parse(o);
  }

  public boolean contains(int val)
  {
    if (withDummy)
      return Arrays.binarySearch(_elements,1, _elements.length, val) >= 0;
    else
      return Arrays.binarySearch(_elements, val) >= 0;
  }
  
  @Override
  public boolean containsWithType(Integer val)
  {
    if (withDummy)
    {
      if (val == null) return false;
      return Arrays.binarySearch(_elements,1, _elements.length, val.intValue()) >= 0;
    } else
    {
      return Arrays.binarySearch(_elements, val.intValue()) >= 0;
    }
  }

  public boolean containsWithType(int val)
  {
    if (withDummy)
      return Arrays.binarySearch(_elements,1, _elements.length, val) >= 0;
    else
      return Arrays.binarySearch(_elements, val) >= 0;
  }

  public int[] getElements() {
    return _elements;
  }
  @Override
  public double getDoubleValue(int index) {    
    return _elements[index];
  }
}
