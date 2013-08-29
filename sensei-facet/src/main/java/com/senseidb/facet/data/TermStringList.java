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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TermStringList extends TermValueList<String> {
  private String sanity = null;
  private boolean withDummy = true;

  public TermStringList(int capacity) {
    super(capacity);
  }

  public TermStringList() {
    this(-1);
  }

	@Override
	public boolean add(String o) {
    if (_innerList.size() == 0 && o!=null) withDummy = false; // the first value added is not null
		if (o==null) o="";
		if (sanity!=null && sanity.compareTo(o)>=0) throw new RuntimeException("Values need to be added in ascending order. Previous value: " + sanity + " adding value: " + o);
		if (_innerList.size() > 0 || !withDummy) sanity = o;
		return ((List<String>)_innerList).add(o);
	}

	@Override
	public boolean addRaw(Object o)
  {
		return add((String)o);
	}

	@Override
	protected List<?> buildPrimitiveList(int capacity) {
	  _type = String.class;
		if (capacity<0)
		{
			return new ArrayList<String>();	
		}
		else
		{
			return new ArrayList<String>(capacity);
		}
	}

	@Override
	public boolean contains(Object o)
	{
	  if (withDummy)
	  {
	    return indexOf(o)>0;
	  } else
	  {
	    return indexOf(o)>=0;
	  }
	}

	@Override
	public String format(Object o) {
		return (String)o;
	}

	@Override
	public int indexOf(Object o)
	{
	  if (withDummy)
	  {
	    if (o == null) return -1;
	   
	    if (o.equals("")) {
        if (_innerList.size() > 1 && "".equals(_innerList.get(1))) {
          return 1;
        } else if (_innerList.size()  < 2) {
          return -1;
        }        
      }  
	    return Collections.binarySearch(((ArrayList<String>)_innerList), (String)o);
	  } else
	  {
      return Collections.binarySearch(((ArrayList<String>)_innerList), (String)o);
	  }
	}

	@Override
	public void seal() {
		((ArrayList<String>)_innerList).trimToSize();
	}

  @Override
  public boolean containsWithType(String val)
  {
    if (withDummy)
    {
      if (val == null) return false;
      if (val.equals("")) {       
          return  _innerList.size() > 1 && "".equals(_innerList.get(1)); 
      }
      return Collections.binarySearch(((ArrayList<String>)_innerList), val)>=0;
    } else
    {
      return Collections.binarySearch(((ArrayList<String>)_innerList), val)>=0;
    } 
  }

  @Override
  public int indexOfWithType(String o)
  {
    if (withDummy)
    {
      if (o == null) return -1;
      if (o.equals("")) {
        if (_innerList.size() > 1 && "".equals(_innerList.get(1))) {
          return 1;
        } else if (_innerList.size() < 2) {
          return -1;
        }        
      }  
      return Collections.binarySearch(((ArrayList<String>)_innerList), (String)o);
    } else
    {
      return Collections.binarySearch(((ArrayList<String>)_innerList), (String)o);
    }
  }

}
