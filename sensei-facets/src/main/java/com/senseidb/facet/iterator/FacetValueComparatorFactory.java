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

package com.senseidb.facet.iterator;

import com.senseidb.facet.Facet;
import com.senseidb.facet.data.IntComparator;
import com.senseidb.facet.handler.FieldValueAccessor;
import com.senseidb.facet.handler.ComparatorFactory;
import com.senseidb.facet.data.BigSegmentedArray;

import java.util.Comparator;

public class FacetValueComparatorFactory implements ComparatorFactory {

  public IntComparator newComparator(FieldValueAccessor fieldValueAccessor, BigSegmentedArray counts) {
    return new IntComparator(){
      public int compare(int o1, int o2) {
        return o2-o1;
      }
    };
  }

	public Comparator<Facet> newComparator() {
		return new Comparator<Facet>(){
			public int compare(Facet o1, Facet o2) {
				return o1.getValue().compareTo(o2.getValue());
			}	
		};
	}
}
