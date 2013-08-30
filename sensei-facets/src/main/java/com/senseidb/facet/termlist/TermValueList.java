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

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

/**
 *  This class behaves as List<String> with a few extensions:
 *  <ul>
 *   <li> Semi-immutable, e.g. once added, cannot be removed. </li>
 *   <li> Assumes sequence of values added are in sorted order </li>
 *   <li> {@link #indexOf(Object)} return value conforms to the contract of {@link java.util.Arrays#binarySearch(Object[], Object)}</li>
 *   <li> {@link #seal()} is introduce to trim the List size, similar to {@link java.util.ArrayList#trimToSize()}, once it is called, no add should be performed.</li>
 *   </u>
 */
public abstract class TermValueList<T> implements List<String>{
	
	protected abstract List<?> buildPrimitiveList(int capacity);
	protected Class<T> _type;
	public abstract String format(Object o);
	public abstract void seal();
	
	protected List<?> _innerList;
	
	protected TermValueList()
	{
		_innerList=buildPrimitiveList(-1);
	}
	
	protected TermValueList(int capacity)
	{
		_innerList=buildPrimitiveList(capacity);
	}
	
	/**
	 * The user of this method should not try to alter the content of the list,
	 * which may result in data inconsistency.
	 * And of the content can be accessed using the getRawValue(int) method.
	 * @return the inner list
	 */
	public List<?> getInnerList(){
		return _innerList;
	}
	
	/**
	 * Add a new value to the list. <b>It is important to add the values in sorted (ASC) order.</b>
	 * Our algorithm uses binary searches and priority queues, both of which fails when the ordering is wrong.
	 */
	abstract public boolean add(String o);

  // do not accept null values
	abstract public boolean addRaw(Object o);

	public void add(int index, String element)
	{
		throw new IllegalStateException("not supported");
	}

	public boolean addAll(Collection<? extends String> c)
	{
	  boolean ret = true;
	  for(String s: c)
	  {
	    ret &= add(s);
	  }
	  return ret;
	}

	public boolean addAll(int index, Collection<? extends String> c)
	{
		throw new IllegalStateException("not supported");
	}

	public void clear() {
		_innerList.clear();
	}

	public boolean contains(Object o){
		return indexOf(o)>=0;
	}
	
	public abstract boolean containsWithType(T val);
	

	public boolean containsAll(Collection<?> c)
	{
		throw new IllegalStateException("not supported");
	}

	public Class<T> getType()
	{
	  return _type;
	}
	public String get(int index) {
		return format(_innerList.get(index));
	}
	
	public T getRawValue(int index){
		return (T) _innerList.get(index);
	}
	public Comparable getComparableValue(int index){    
	  return (Comparable) _innerList.get(index);
  }
	abstract public int indexOf(Object o);

  public int indexOfWithOffset(Object value, int offset)
  {
    throw new IllegalStateException("not supported");
  }

  public abstract int indexOfWithType(T o);

  public boolean isEmpty() {
		return _innerList.isEmpty();
	}

	public Iterator<String> iterator() {
		final Iterator<?> iter=_innerList.iterator();
		
		return new Iterator<String>()
		{
			public boolean hasNext() {
				return iter.hasNext();
			}

			public String next() {
				return format(iter.next());
			}

			public void remove() {
				iter.remove();
			}
		};
	}

	public int lastIndexOf(Object o)
	{
		return indexOf(o);
	}

	public ListIterator<String> listIterator()
	{
		throw new IllegalStateException("not supported");
	}

	public ListIterator<String> listIterator(int index)
	{
		throw new IllegalStateException("not supported");
	}

	public boolean remove(Object o)
	{
		throw new IllegalStateException("not supported");
	}

	public String remove(int index) {
		throw new IllegalStateException("not supported");
	}

	public boolean removeAll(Collection<?> c)
	{
		throw new IllegalStateException("not supported");
	}

	public boolean retainAll(Collection<?> c)
	{
		throw new IllegalStateException("not supported");
	}

	public String set(int index, String element)
	{
		throw new IllegalStateException("not supported");
	}

	public int size() {
		return _innerList.size();
	}

	public List<String> subList(int fromIndex, int toIndex) {
		throw new IllegalStateException("not supported");
	}

	public Object[] toArray() {
		Object[] array=_innerList.toArray();
		Object[] retArray=new Object[array.length];
		for (int i=0;i<array.length;++i)
		{
			retArray[i]=format(array[i]);
		}
		return retArray;
	}

	public <T> T[] toArray(T[] a) {
		List<String> l = subList(0,size());
		return l.toArray(a);
	}
	
	public static void main(String[] args) {
		int numIter = 20000;
		TermIntList list = new TermIntList();
		for (int i=0;i<numIter;++i){
			list.add(String.valueOf(i));
		}
		long start = System.currentTimeMillis();
		List<?> rawList = list.getInnerList();
		for (int i=0;i<numIter;++i){
			rawList.get(i);
		}
		long end = System.currentTimeMillis();
		System.out.println("took: "+(end-start));
	}
}
