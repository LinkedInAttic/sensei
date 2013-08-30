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

package com.senseidb.facet.termlist;

import it.unimi.dsi.fastutil.longs.LongArrayList;

import java.util.Iterator;
import java.util.List;

public class TermFixedLengthLongArrayList extends TermValueList<long[]>
{
  protected long[] _elements = null;
  protected int width;

  private long[] sanity;
  private boolean withDummy = true;

  public TermFixedLengthLongArrayList(int width)
  {
    super();
    this.width = width;
    sanity = new long[width];
    sanity[width-1] = -1;
  }

  public TermFixedLengthLongArrayList(int width, int capacity)
  {
    super(capacity * width);
    this.width = width;
    sanity = new long[width];
    sanity[width-1] = -1;
  }

  protected long[] parse(String s)
  {
    long[] r = new long[width];

    if (s == null  || s.length() == 0)
      return r;

    String[] a = s.split(",");
    if (a.length != width)
      throw new RuntimeException(s + " is not a " + width + " fixed width long.");

    for (int i=0; i<width; ++i)
    {
      r[i] = Long.parseLong(a[i]);
      //if (r[i] < 0)
       // throw new RuntimeException("We only support non-negative numbers: " + s);
    }

    return r;
  }

  @Override
  public boolean add(String o)
  {
    if (_innerList.size() == 0 && o!=null) // the first value added is not null
      withDummy = false;

    return addRaw(parse(o));
  }

	@Override
	public boolean addRaw(Object o)
  {
    int i = 0;
    long cmp = 0;

    long[] item = (long[])o;

    for(i=0; i<width; ++i)
    {
      cmp = item[i] - sanity[i];
      if (cmp != 0)
        break;
    }

    //if (cmp<=0)
    //  throw new RuntimeException("Values need to be added in ascending order and we only support non-negative numbers: " + o);

    for(i=0; i<width; ++i)
    {
      if(!((LongArrayList) _innerList).add(item[i]))
      {
        if (i>0)
        {
          ((LongArrayList) _innerList).removeElements(_innerList.size()-i,
                                                      _innerList.size()-1);
        }
        return false;
      }
    }

    if (_innerList.size() > width || !withDummy)
      for(i=0; i<width; ++i)
        sanity[i] = item[i];

    return true;
	}

  @Override
  protected List<?> buildPrimitiveList(int capacity)
  {
    _type = long[].class;
    return capacity > 0 ? new LongArrayList(capacity) : new LongArrayList();
  }

  @Override
  public void clear()
  {
    super.clear();
  }

  @Override
  public String get(int index)
  {
    index = index * width;
    StringBuilder sb = new StringBuilder();
    sb.append(_elements[index]);

    int left = width;
    ++index;
    while (left>1)
    {
      sb.append(',');
      sb.append(_elements[index]);
      --left; ++index;
    }

    return sb.toString();
  }

  public long[] getRawValue(int index){
    long[] val = new long[width];

    index = index * width;
    for (int i=0; i<width; ++i)
    {
      val[i] = _elements[index+i];
    }

    return val;
  }

  public Iterator<String> iterator() {
    final Iterator<?> iter=_innerList.iterator();
    
    return new Iterator<String>()
    {
      public boolean hasNext() {
        return iter.hasNext();
      }

      public String next() {
        long[] val = new long[width];
        for(int i=0; i<width; ++i)
        {
          val[i] = (Long)iter.next();
        }
        return format(val);
      }

      public void remove() {
        for(int i=0; i<width; ++i)
          iter.remove();
      }
    };
  }

  public int size() {
    return _innerList.size()/width;
  }

  public Object[] toArray() {
    Object[] retArray=new Object[size()];
    for (int i=0; i<retArray.length; ++i)
    {
      retArray[i] = get(i);
    }
    return retArray;
  }

  public long[][] toArray(long[][] a) {
    long[][] retArray=new long[size()][];
    for (int i=0; i<retArray.length; ++i)
    {
      retArray[i] = getRawValue(i);
    }
    return retArray;
  }

  @Override
  public String format(Object o)
  {
    if (o == null)
      return null;

    if (o instanceof String)
      o = parse((String)o);

    long[] val = (long[])o;

    if (val.length == 0)
      return null;

    StringBuilder sb = new StringBuilder();
    sb.append(val[0]);

    for(int i=1; i<val.length; ++i)
    {
      sb.append(',');
      sb.append(val[i]);
    }
    return sb.toString();
  }

  public long[] getPrimitiveValue(int index)
  {
    index = index * width;
    long[] r = new long[width];

    if (index < _elements.length)
    {
      for(int i=0; i<width; ++i, ++index)
        r[i] = _elements[index];
    }
    else
    {
      r[width-1] = -1;
    }
    return r;
  }

  protected int binarySearch(long[] key)
  {
    return binarySearch(key, 0, _elements.length/width -1);
  }

  protected int binarySearch(long[] key, int low, int high)
  {
    int mid=0;
    long cmp = -1;
    int index, i;

    while(low <= high)
    {
      mid = (low+high)/2;
      index = mid * width;
      for (i=0; i<width; ++i, ++index)
      {
        cmp = key[i]-_elements[index];

        if (cmp != 0)
          break;
      }
      if (cmp > 0)
        low = mid+1;
      else if (cmp < 0)
        high = mid-1;
      else
        return mid;
    }
    return -(mid+1);
  }

  @Override
  public int indexOf(Object o)
  {
    if (withDummy)
    {
      if (o instanceof String)
        o = parse((String)o);
      return binarySearch((long[])o, 1, _elements.length/width - 1);
    }
    else
    {
      if (o instanceof String)
        o = parse((String)o);
      return binarySearch((long[])o);
    }
  }

  public int indexOf(long[] val)
  {
    if (withDummy)
    {
      return binarySearch(val, 1, _elements.length/width - 1);
    }
    else
      return binarySearch(val);
  }

  public int indexOfWithType(long[] val)
  {
    if (withDummy)
    {
      return binarySearch(val, 1, _elements.length/width - 1);
    }
    else
      return binarySearch(val);
  }
  @Override
  public Comparable getComparableValue(int index) {    
    return get(index);
  }
  @Override
  public void seal()
  {
    ((LongArrayList) _innerList).trim();
    _elements = ((LongArrayList) _innerList).elements();
  }

  public boolean contains(long[] val)
  {
    if (withDummy)
    {
      return binarySearch(val, 1, _elements.length/width - 1) >= 0;
    }
    else
      return binarySearch(val) >= 0;
  }
  
  public boolean containsWithType(long[] val)
  {
    if (withDummy)
    {
      return binarySearch(val, 1, _elements.length/width - 1) >= 0;
    }
    else
      return binarySearch(val) >= 0;
  }
}


