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

package com.senseidb.facet.handler;

import com.senseidb.facet.Facet;
import com.senseidb.facet.data.BigSegmentedArray;
import com.senseidb.facet.data.IntComparator;

import java.util.Comparator;

/**
 * Comparator for custom sorting a facet value
 * @author jwang
 */
public interface ComparatorFactory {
	/**
	 * Providers a Comparator from field values and counts. This is called within a browse.
	 * @param fieldValueAccessor accessor for field values
	 * @param counts hit counts
	 * @return Comparator instance
	 */
  IntComparator newComparator(FieldValueAccessor fieldValueAccessor, BigSegmentedArray counts);
	
	/**
	 * Providers a Comparator. This is called when doing a merge across browses.
	 * @return Comparator instance
	 */
	Comparator<Facet> newComparator();
}
