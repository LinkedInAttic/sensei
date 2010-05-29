package com.sensei.search.client;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.browseengine.bobo.api.BrowseFacet;
import com.browseengine.bobo.api.FacetAccessible;
import com.browseengine.bobo.api.FacetIterator;
import com.browseengine.bobo.api.FacetSpec;
import com.browseengine.bobo.util.ListMerger;
import com.sensei.search.req.SenseiHit;
import com.sensei.search.req.SenseiRequest;
import com.sensei.search.req.SenseiResult;

public class ResultMerger {
	private static Map<String,FacetAccessible> mergeFacetContainer(Collection<Map<String,FacetAccessible>> subMaps,SenseiRequest req)
    {
      Map<String, Map<String, Integer>> counts = new HashMap<String, Map<String, Integer>>();
      for (Map<String,FacetAccessible> subMap : subMaps)
      {
        for(Map.Entry<String, FacetAccessible> entry : subMap.entrySet())
        {
          Map<String, Integer> count = counts.get(entry.getKey());
          if(count == null)
          {
            count = new HashMap<String, Integer>();
            counts.put(entry.getKey(), count);
          }
          for(BrowseFacet facet : entry.getValue().getFacets())
          {
            String val = facet.getValue();
            int oldValue = count.containsKey(val) ? count.get(val) : 0;
            count.put(val, oldValue + facet.getHitCount());
          }
        }
      }

      Map<String, FacetAccessible> mergedFacetMap = new HashMap<String, FacetAccessible>();
      for(String facet : counts.keySet())
      {
        Map<String, Integer> facetValueCounts = counts.get(facet);
        List<BrowseFacet> facets = new ArrayList<BrowseFacet>(facetValueCounts.size());
        for(Entry<String, Integer> entry : facetValueCounts.entrySet())
        {
          facets.add(new BrowseFacet(entry.getKey(), entry.getValue()));
        }
        Collections.sort(facets, new Comparator<BrowseFacet>()
        {
          public int compare(BrowseFacet f1, BrowseFacet f2)
          {
            int h1 = f1.getHitCount();
            int h2 = f2.getHitCount();

            int val = h2 - h1;

            if (val == 0)
            {
              val = f1.getValue().compareTo(f2.getValue());
            }
            return val;
          }
        });
        if (req != null)
        {
          FacetSpec fspec = req.getFacetSpec(facet);
          if (fspec!=null){
            int maxCount = fspec.getMaxCount();
            int numToShow = facets.size();
            if (maxCount>0){
                    numToShow = Math.min(maxCount,numToShow);
            }
            facets = facets.subList(0, numToShow);
          }
        }
        MappedFacetAccessible mergedFacetAccessible = new MappedFacetAccessible(facets.toArray(new BrowseFacet[facets.size()]));
        mergedFacetMap.put(facet, mergedFacetAccessible);
      }
      return mergedFacetMap;
    }

	private static class MappedFacetAccessible implements FacetAccessible, Serializable {

        /**
         * 
         */
        private static final long serialVersionUID = 1L;
        
        private final HashMap<String,BrowseFacet> _facetMap;
        private final BrowseFacet[] _facets;
        
        public MappedFacetAccessible(BrowseFacet[] facets){
                _facetMap = new HashMap<String,BrowseFacet>();
                for (BrowseFacet facet : facets){
                        _facetMap.put(facet.getValue(), facet);
                }
                _facets = facets;
        }

        public BrowseFacet getFacet(String value) {
                return _facetMap.get(value);
        }

        public List<BrowseFacet> getFacets() {
                return Arrays.asList(_facets);
        }

		@Override
		public void close() {
			// TODO Auto-generated method stub
			
		}

		@Override
		public FacetIterator iterator() {
			throw new IllegalStateException("FacetIterator should not be obtained at merge time");
		}

	}

	
	public static SenseiResult merge(SenseiRequest req,Collection<SenseiResult> results){
		List<Map<String,FacetAccessible>> facetList=new ArrayList<Map<String,FacetAccessible>>(results.size());
        
        ArrayList<Iterator<SenseiHit>> iteratorList = new ArrayList<Iterator<SenseiHit>>(results.size());
        int numHits = 0;
        int totalDocs = 0;
        
        for (SenseiResult res : results){
        	SenseiHit[] hits = res.getSenseiHits();
        	if (hits!=null){
        		for (SenseiHit hit : hits){
        			hit.setDocid(hit.getDocid() + totalDocs);
        		}
        	}
        	numHits += res.getNumHits();
        	totalDocs += res.getTotalDocs();
        	Map<String,FacetAccessible> facetMap = res.getFacetMap();
        	if (facetMap != null){
        		facetList.add(facetMap);
        	}
        	iteratorList.add(Arrays.asList(res.getSenseiHits()).iterator());
        }
        
        Map<String,FacetAccessible> mergedFacetMap = mergeFacetContainer(facetList,req);
        Comparator<SenseiHit> comparator = new Comparator<SenseiHit>(){

			public int compare(SenseiHit o1, SenseiHit o2) {
				Comparable c1=o1.getComparable();
				Comparable c2=o2.getComparable();
				if (c1==null || c2==null){
					return o2.getDocid() - o1.getDocid();
				}
				return c1.compareTo(c2);
			}
	    	
	    };
	    
        ArrayList<SenseiHit> mergedList = ListMerger.mergeLists(req.getOffset(), req.getCount(), iteratorList.toArray(new Iterator[iteratorList.size()]), comparator);
        SenseiHit[] hits = mergedList.toArray(new SenseiHit[mergedList.size()]);
        
        SenseiResult merged = new SenseiResult();
        merged.setHits(hits);
        merged.setNumHits(numHits);
        merged.setTotalDocs(totalDocs);
        merged.addAll(mergedFacetMap);
        return merged;
	}
}
