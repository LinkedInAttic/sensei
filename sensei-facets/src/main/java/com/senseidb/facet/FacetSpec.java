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

package com.senseidb.facet;

import com.senseidb.facet.handler.ComparatorFactory;


/**
 * specifies how facets are to be returned for a browse
 *
 */
public class FacetSpec
{
	/**
	 * Sort options for facets
	 */
	public static enum FacetSortSpec
	{
	  /**
	   * Order by the _facet values in lexicographical ascending order
	   */
		OrderValueAsc,
		
		/**
		 * Order by the _facet hit counts in descending order
		 */
		OrderHitsDesc,
		
		/**
		 * custom order, must have a comparator
		 */
		OrderByCustom
	}
			
	private FacetSortSpec _orderBy;
	private int _max;
	private boolean _expandSelection;
	private int _minCount;
	private ComparatorFactory _comparatorFactory;

	public FacetSpec()
  {
		_orderBy = FacetSortSpec.OrderValueAsc;
		_minCount =1;
		_expandSelection = false;
		_comparatorFactory = null;
	}				
	
	public FacetSpec setCustomComparatorFactory(ComparatorFactory comparatorFactory)
  {
		_comparatorFactory = comparatorFactory;
    return this;
	}
	
	public ComparatorFactory getCustomComparatorFactory()
  {
		return _comparatorFactory;
	}
	
  /**
	 * Sets the minimum number of hits a choice would need to have to be returned.
	 *
   * @param minCount minimum _count
   * @see #getMinHitCount()
	*/
	public FacetSpec setMinHitCount(int minCount){
		this._minCount = minCount;
    return this;
	}
	
	/**
	 * Gets the minimum number of hits a choice would need to have to be returned.
	 * @return minimum _count
	 * @see #setMinHitCount(int)
	 */
	public int getMinHitCount()
  {
		return _minCount;
	}
	
	/**
	 * Get the current choice sort order
	 * @return choice sort order
	 * @see #setOrderBy(FacetSortSpec)
	 */
	public FacetSortSpec getOrderBy()
  {
		return _orderBy;
	}

	/**
	 * Sets the choice sort order
	 *
   * @param order sort order
   * @see #getOrderBy()
	 */
	public FacetSpec setOrderBy(FacetSortSpec order)
  {
		_orderBy = order;
    return this;
	}				

	/**
	 * Gets the maximum number of choices to return
	 * @return _max number of choices to return
	 * @see #setMaxCount(int)
	 */
	public int getMaxCount()
  {
		return _max;
	}

	/**
	 * Sets the maximum number of choices to return.
	 *
   * @param maxCount _max number of choices to return, default = 0 which means all
   * @see #getMaxCount()
	 */
	public FacetSpec setMaxCount(int maxCount)
  {
		_max = maxCount;
    return this;
	}

	/**
	 * Gets whether we are expanding sibling choices
	 * @return A boolean indicating whether to expand sibling choices.
	 * @see #setExpandSelection(boolean)
	 */
	public boolean isExpandSelection()
  {
		return _expandSelection;
	}

	/**
	 * Sets whether we are expanding sibling choices
	 *
   * @param expandSelection indicating whether to expand sibling choices.
   * @see #isExpandSelection()
	 */
	public FacetSpec setExpandSelection(boolean expandSelection)
  {
		this._expandSelection = expandSelection;
    return this;
	}
}
