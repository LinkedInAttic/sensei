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
package com.senseidb.search.facet.attribute;

import com.browseengine.bobo.facets.data.FacetDataCache;
import com.browseengine.bobo.facets.data.MultiValueFacetDataCache;

public class RangePredicate implements  FacetPredicate {
 
  
  private final String value;
  private final char separator;
  private MultiValueFacetDataCache lastDataCache;
  private Range range;
  private int[] buffer;
  private final Range getRange(FacetDataCache cache) {
    if (cache == lastDataCache) {
      return range;
    }    
    lastDataCache = (MultiValueFacetDataCache) cache;   
    range = Range.getRange(lastDataCache, value, separator);   
    buffer = new int[lastDataCache._nestedArray.getMaxItems()];
    return range;
  }  
  
  public RangePredicate(String val, char separator) {
    value = val;
    this.separator = separator;   
  }  
  
  @Override
  public boolean evaluate(FacetDataCache cache, int docId) {
    if (cache != lastDataCache) {
      getRange(cache);
    }
   return lastDataCache._nestedArray.containsValueInRange(docId, range.start, range.end);
  }

  @Override
  public boolean evaluateValue(FacetDataCache cache, int valId) {
    if (cache != lastDataCache) {
      getRange(cache);
    }
    return valId >= range.start && valId < range.end;
  } 

  @Override
  public int valueStartIndex(FacetDataCache cache) {    
    if (cache != lastDataCache) {
      getRange(cache);
    }
    return range.start;
  }
  @Override
  public int valueEndIndex(FacetDataCache cache) {    
    return range.end;
  } 
}
