package com.sensei.search.client;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import com.browseengine.bobo.api.BrowseFacet;
import com.browseengine.bobo.api.BrowseSelection;
import com.browseengine.bobo.api.FacetAccessible;
import com.browseengine.bobo.api.FacetIterator;
import com.browseengine.bobo.api.FacetSpec;
import com.browseengine.bobo.api.FacetSpec.FacetSortSpec;
import com.browseengine.bobo.facets.CombinedFacetAccessible;
import com.browseengine.bobo.util.ListMerger;
import com.sensei.search.req.SenseiHit;
import com.sensei.search.req.SenseiRequest;
import com.sensei.search.req.SenseiResult;

public class ResultMerger
{
  private final static Logger logger = Logger.getLogger(ResultMerger.class.getName());
  private static Map<String, FacetAccessible> mergeFacetContainer(Collection<Map<String, FacetAccessible>> subMaps,
      SenseiRequest req)
  {
    Map<String, Map<String, Integer>> counts = new HashMap<String, Map<String, Integer>>();
    for (Map<String, FacetAccessible> subMap : subMaps)
    {
      for (Map.Entry<String, FacetAccessible> entry : subMap.entrySet())
      {
        String facetname = entry.getKey();
        Map<String, Integer> count = counts.get(facetname);
        if(count == null)
        {
          count = new HashMap<String, Integer>();
          counts.put(facetname, count);
        }
        Set<String> values = new HashSet<String>();
        String[] rawvalues = null;
        BrowseSelection selection = req.getSelection(facetname);
        if (selection!=null&&(rawvalues = selection.getValues())!=null)
        {
          values.addAll(Arrays.asList(rawvalues));
        }
        FacetAccessible facetAccessible = entry.getValue();
        for(BrowseFacet facet : facetAccessible.getFacets())
        {
	      if (facet == null) continue;
          String val = facet.getValue();
          int oldValue = count.containsKey(val) ? count.get(val) : 0;
          count.put(val, oldValue + facet.getFacetValueHitCount());
          values.remove(val);
        }
        if (!values.isEmpty())
        {
          for(String val : values)
          {
            int oldValue = count.containsKey(val) ? count.get(val) : 0;
            BrowseFacet facet = facetAccessible.getFacet(val);
            int delta = 0;
            if (facet!=null)
            {
              delta = facet.getFacetValueHitCount();
            }
            count.put(val, oldValue + delta);
          }
        }
        facetAccessible.close();
      }
    }

    Map<String, FacetAccessible> mergedFacetMap = new HashMap<String, FacetAccessible>();
    for (String facet : counts.keySet())
    {
      Map<String, Integer> facetValueCounts = counts.get(facet);
      List<BrowseFacet> facets = new ArrayList<BrowseFacet>(facetValueCounts.size());
      for (Entry<String, Integer> entry : facetValueCounts.entrySet())
      {
        facets.add(new BrowseFacet(entry.getKey(), entry.getValue()));
      }
      FacetSpec fspec = null;
      Set<String> values = new HashSet<String>();
      String[] rawvalues = null;
      if (req != null)
      {
        fspec = req.getFacetSpec(facet);
        BrowseSelection selection = req.getSelection(facet);
        if (selection!=null&&(rawvalues = selection.getValues())!=null)
        {
          values.addAll(Arrays.asList(rawvalues));
        }
      }
      Comparator<BrowseFacet> facetComp = getComparator(fspec);
      Collections.sort(facets, facetComp);
      if (fspec != null)
      {
        int maxCount = fspec.getMaxCount();
        int numToShow = facets.size();
        if (maxCount > 0)
        {
          numToShow = Math.min(maxCount, numToShow);
        }
        for(int i = facets.size() - 1; i >= numToShow; i--)
        {
          if (!values.contains(facets.get(i).getValue()))
          {
            facets.remove(i);
          }
        }
      }
      MappedFacetAccessible mergedFacetAccessible = new MappedFacetAccessible(facets.toArray(new BrowseFacet[facets.size()]));
      mergedFacetMap.put(facet, mergedFacetAccessible);
    }
    return mergedFacetMap;
  }
  private static Map<String, FacetAccessible> mergeFacetContainerServerSide(Collection<Map<String, FacetAccessible>> subMaps, SenseiRequest req)
  {
    Map<String, List<FacetAccessible>> counts = new HashMap<String, List<FacetAccessible>>();
    for (Map<String, FacetAccessible> subMap : subMaps)
    {
      for (Map.Entry<String, FacetAccessible> entry : subMap.entrySet())
      {
        String facetname = entry.getKey();
        List<FacetAccessible> count = counts.get(facetname);
        if(count == null)
        {
          count = new LinkedList<FacetAccessible>();
          counts.put(facetname, count);
        }
        count.add(entry.getValue());
      }
    }
    // create combinedFacetAccessibles
    Map<String, FacetAccessible> fieldMap = new HashMap<String, FacetAccessible>();
    for(String fieldname : counts.keySet())
    {
      List<FacetAccessible> facetAccs = counts.get(fieldname);
      if (facetAccs.size() == 1)
      {
        fieldMap.put(fieldname, facetAccs.get(0));
      } else
      {
        fieldMap.put(fieldname, new CombinedFacetAccessible(req.getFacetSpec(fieldname), facetAccs));
      }
    }
    Map<String, FacetAccessible> mergedFacetMap = new HashMap<String, FacetAccessible>();
    for(String fieldname : fieldMap.keySet())
    {
      FacetAccessible facetAcc = fieldMap.get(fieldname);
      FacetSpec fspec = req.getFacetSpec(fieldname);
      BrowseSelection sel = req.getSelection(fieldname);
      Set<String> values = new HashSet<String>();
      String[] rawvalues = null;
      if (sel!=null&&(rawvalues = sel.getValues())!=null)
      {
        values.addAll(Arrays.asList(rawvalues));
      }
      List<BrowseFacet> facets = new ArrayList<BrowseFacet>();
      facets.addAll(facetAcc.getFacets());
      for(BrowseFacet bf : facets)
      {
        values.remove(bf.getValue());
      }
      if (values.size()>0)
      {
        for(String value : values)
        {
          facets.add(facetAcc.getFacet(value));
        }
      }
      facetAcc.close();
      // sorting
      Comparator<BrowseFacet> facetComp = getComparator(fspec);
      Collections.sort(facets, facetComp);
      MappedFacetAccessible mergedFacetAccessible = new MappedFacetAccessible(facets.toArray(new BrowseFacet[facets.size()]));
      mergedFacetMap.put(fieldname, mergedFacetAccessible);
    }
    return mergedFacetMap;
  }


  private static Comparator<BrowseFacet> getComparator(FacetSpec fspec)
  {
    Comparator<BrowseFacet> facetComp;
    if ((fspec == null) || fspec.getOrderBy() == FacetSortSpec.OrderHitsDesc)
    {
      facetComp = new BrowseFacetHitsDescComparator();
    } else
    {
      if (fspec.getOrderBy() == FacetSortSpec.OrderValueAsc)
      {
        facetComp = new BrowseFacetValueAscComparator();
      } else
      {
        facetComp = fspec.getCustomComparatorFactory().newComparator();
      }
    }
    return facetComp;
  }

  private static final class BrowseFacetValueAscComparator implements Comparator<BrowseFacet>
  {
    public int compare(BrowseFacet f1, BrowseFacet f2)
    {
		if (f1==null && f2==null){
		    return 0;	
		  }
		  if (f1==null){
		    return -1;	
		  }
		  if (f2==null){
		    return 1;	
		  }
      return f1.getValue().compareTo(f2.getValue());
    }
  }

  private static final class BrowseFacetHitsDescComparator implements Comparator<BrowseFacet>
  {
    public int compare(BrowseFacet f1, BrowseFacet f2)
    {
	  if (f1==null && f2==null){
	    return 0;	
	  }
	  if (f1==null){
	    return -1;	
	  }
	  if (f2==null){
	    return 1;	
	  }
      int h1 = f1.getFacetValueHitCount();
      int h2 = f2.getFacetValueHitCount();

      int val = h2 - h1;

      if (val == 0)
      {
        val = f1.getValue().compareTo(f2.getValue());
      }
      return val;
    }
  }

  private static final class SenseiHitComparator implements Comparator<SenseiHit>
  {
    public int compare(SenseiHit o1, SenseiHit o2)
    {
      Comparable c1 = o1.getComparable();
      Comparable c2 = o2.getComparable();
      if (c1 == null || c2 == null)
      {
        return o2.getDocid() - o1.getDocid();
      }
      return c1.compareTo(c2);
    }
  }

  private static class MappedFacetAccessible implements FacetAccessible, Serializable
  {

    /**
         * 
         */
    private static final long serialVersionUID = 1L;

    private final HashMap<String, BrowseFacet> _facetMap;
    private final BrowseFacet[] _facets;

    public MappedFacetAccessible(BrowseFacet[] facets)
    {
      _facetMap = new HashMap<String, BrowseFacet>();
      for (BrowseFacet facet : facets)
      {
	    if (facet!=null){
          _facetMap.put(facet.getValue(), facet);
        }
      }
      _facets = facets;
    }

    public BrowseFacet getFacet(String value)
    {
      return _facetMap.get(value);
    }

    public List<BrowseFacet> getFacets()
    {
      return Arrays.asList(_facets);
    }

    @Override
    public void close()
    {
      // TODO Auto-generated method stub

    }

    @Override
    public FacetIterator iterator()
    {
      throw new IllegalStateException("FacetIterator should not be obtained at merge time");
    }

  }

  public static SenseiResult merge(SenseiRequest req, Collection<SenseiResult> results, boolean onSearchNode)
  {
	long start = System.currentTimeMillis();
    List<Map<String, FacetAccessible>> facetList = new ArrayList<Map<String, FacetAccessible>>(results.size());

    ArrayList<Iterator<SenseiHit>> iteratorList = new ArrayList<Iterator<SenseiHit>>(results.size());
    int numHits = 0;
    int totalDocs = 0;

    long time = 0L;
    
    String parsedQuery = null;
    for (SenseiResult res : results)
    {
      parsedQuery = res.getParsedQuery();
      SenseiHit[] hits = res.getSenseiHits();
      if (hits != null)
      {
        for (SenseiHit hit : hits)
        {
          hit.setDocid(hit.getDocid() + totalDocs);
        }
      }
      numHits += res.getNumHits();
      totalDocs += res.getTotalDocs();
      if (onSearchNode)
      { // no server side sum the time, since thing are done in sequence
        time += res.getTime();
      } else
      { // no client side take max
        time = Math.max(time,res.getTime());
      }
      Map<String, FacetAccessible> facetMap = res.getFacetMap();
      if (facetMap != null)
      {
        facetList.add(facetMap);
      }
      iteratorList.add(Arrays.asList(res.getSenseiHits()).iterator());
    }

    Map<String, FacetAccessible> mergedFacetMap = null;
    if (onSearchNode)
    {
      mergedFacetMap = mergeFacetContainerServerSide(facetList, req);
    } else
    {
      mergedFacetMap = mergeFacetContainer(facetList, req);
    }
    Comparator<SenseiHit> comparator = new SenseiHitComparator();

    ArrayList<SenseiHit> mergedList = ListMerger.mergeLists(req.getOffset(), req.getCount(), iteratorList
        .toArray(new Iterator[iteratorList.size()]), comparator);
    SenseiHit[] hits = mergedList.toArray(new SenseiHit[mergedList.size()]);

    SenseiResult merged = new SenseiResult();
    merged.setHits(hits);
    merged.setNumHits(numHits);
    merged.setTotalDocs(totalDocs);
    merged.addAll(mergedFacetMap);
    
    if (parsedQuery == null){
    	parsedQuery = "";
    }
    
    long end = System.currentTimeMillis();
    
    time += (end-start);
    merged.setTime(time);
    merged.setParsedQuery(parsedQuery);
    return merged;
  }
}
