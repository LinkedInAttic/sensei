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
package com.senseidb.search.req.mapred;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.browseengine.bobo.facets.FacetCountCollector;

/**
 * 
 * Allows to access facet counts. If there are no facets specified in the Sensei request, the method areFacetCountsPresent would always return false and 
 * there is no point of using this class.
 * In the SenseiMapReduce.map method facetCounts might not always be present. Generally it depends on the length of the segment 
 * so the developer should always perform a check areFacetCountsPresent. Facet Counts would be available only if the map method is called at the end of the Lucene Segment
 * 
 *  @author vzhabiuk
 *
 */
public class FacetCountAccessor {
    private Map<String, FacetCountCollector> facetCountCollectors = new HashMap<String, FacetCountCollector>();;
    public FacetCountAccessor(FacetCountCollector[] facetCountCollectors) {
      if (facetCountCollectors != null) {
        for (FacetCountCollector facetCountCollector : facetCountCollectors) {
          this.facetCountCollectors.put(facetCountCollector.getName(), facetCountCollector);
        }
      }
    }
    
    /**
     * @return true if facets are in the request and the map method is called at the end of the Lucene segment
     */
    public boolean areFacetCountsPresent() {
      return !facetCountCollectors.isEmpty();
    }
    
   
    /**
     * @param facetName
     * @param value
     * @return facet count or -1 if facet doesn't exist or facet value can not be found
     */
    public int getFacetCount(String facetName, Object value) {
      if (!facetCountCollectors.containsKey(facetName)) {
        return -1;
      }
      return facetCountCollectors.get(facetName).getFacetHitsCount(value);
    }
    /**
     * @param facetName
     * @param value
     * @return facet count or -1 if facet doesn't exist or facet value can not be found
     */
    public int getFacetCount(String facetName, String value) {
      if (!facetCountCollectors.containsKey(facetName)) {
        return -1;
      }
      return facetCountCollectors.get(facetName).getFacet(value).getFacetValueHitCount();
    }
    /**
     * @param facetName
     * @param valIndex
     * @return facet count or -1 if facet doesn't exist or facet value can not be found
     */
    public int  getFacetCount(String facetName, int valIndex) {
      if (!facetCountCollectors.containsKey(facetName)) {
        return -1;
      }
      return facetCountCollectors.get(facetName).getCountDistribution()[valIndex];
    }
    /**Returns the Bobo specific class that is responsible for facet counting
     * @param facetName
     * @return 
     */
    public FacetCountCollector getFacetCollector(String facetName) {      
      return facetCountCollectors.get(facetName);
    }
    public Set<FacetCountCollector> getFacetCountCollectors() {
      return new HashSet<FacetCountCollector>(facetCountCollectors.values());
    }
    public static final FacetCountAccessor EMPTY = new FacetCountAccessor(new FacetCountCollector[0]);
}
