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
      return facetCountCollectors.get(facetName).getCountDistribution().get(valIndex);
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
