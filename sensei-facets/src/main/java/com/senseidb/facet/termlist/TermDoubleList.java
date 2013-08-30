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

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;

import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.List;

public class TermDoubleList extends TermNumberList<Double>
{

  private double[] _elements = null;
  public static final double VALUE_MISSING = Double.MIN_VALUE;
  private static double parse(String s)
  {
    if (s == null || s.length() == 0)
    {
      return 0.0;
    } else
    {
      return Double.parseDouble(s);
    }
  }

  public TermDoubleList()
  {
    super();
  }

  public TermDoubleList(String formatString)
  {
    super(formatString);
  }

  public TermDoubleList(int capacity, String formatString)
  {
    super(capacity, formatString);
  }

  @Override
  public boolean add(String o)
  {
    return ((DoubleArrayList) _innerList).add(parse(o));
  }

	@Override
	public boolean addRaw(Object o)
  {
		return ((DoubleArrayList) _innerList).add(((Number)o).doubleValue());
	}

  @Override
  protected List<?> buildPrimitiveList(int capacity)
  {
    _type = Double.class;
    return capacity > 0 ? new DoubleArrayList(capacity) : new DoubleArrayList();
  }

  @Override
  public String get(int index)
  {
    DecimalFormat formatter = _formatter.get();
    if (formatter == null)
      return String.valueOf(_elements[index]);
    return formatter.format(_elements[index]);
  }

  public double getPrimitiveValue(int index)
  {
    if (index < _elements.length)
      return _elements[index];
    else
      return VALUE_MISSING;
  }

  @Override
  public int indexOf(Object o)
  {
    double val = parse((String) o);
    if (o instanceof String)
      val = parse((String)o);
    else
      val = (Double)o;
    double[] elements = ((DoubleArrayList) _innerList).elements();
    return Arrays.binarySearch(elements, val);
  }

  public int indexOf(double val)
  {
    return Arrays.binarySearch(_elements, val);
  }

  @Override
  public void seal()
  {
    ((DoubleArrayList) _innerList).trim();
    _elements = ((DoubleArrayList) _innerList).elements();
    int negativeIndexCheck =  1;
    //reverse negative elements, because string order and numeric orders are completely opposite
    if (_elements.length > negativeIndexCheck && _elements[negativeIndexCheck] < 0) {
      int endPosition = indexOfWithType((short) 0);
      if (endPosition < 0) {
        endPosition = -1 *endPosition - 1;
      }
      double tmp;
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

  public boolean contains(double val)
  {
    return Arrays.binarySearch(_elements, val) >= 0;
  }

  @Override
  public boolean containsWithType(Double val)
  {
    return Arrays.binarySearch(_elements, val) >= 0;
  }

  public boolean containsWithType(double val)
  {
    return Arrays.binarySearch(_elements, val) >= 0;
  }

  @Override
  public int indexOfWithType(Double o)
  {
    return Arrays.binarySearch(_elements, o);
  }

  public int indexOfWithType(double val)
  {
    return Arrays.binarySearch(_elements, val);
  }
  @Override
  public double getDoubleValue(int index) {    
    return _elements[index];
  }
}
