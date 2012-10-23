package com.senseidb.search.node;

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
import java.util.Map.Entry;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.PriorityQueue;

import proj.zoie.api.ZoieIndexReader;

import com.browseengine.bobo.api.BoboIndexReader;
import com.browseengine.bobo.api.BrowseFacet;
import com.browseengine.bobo.api.BrowseSelection;
import com.browseengine.bobo.api.FacetAccessible;
import com.browseengine.bobo.api.FacetIterator;
import com.browseengine.bobo.api.FacetSpec;
import com.browseengine.bobo.api.FacetSpec.FacetSortSpec;
import com.browseengine.bobo.facets.CombinedFacetAccessible;
import com.browseengine.bobo.facets.FacetHandler;
import com.browseengine.bobo.facets.data.FacetDataCache;
import com.browseengine.bobo.facets.data.PrimitiveLongArrayWrapper;
import com.browseengine.bobo.sort.DocComparator;
import com.browseengine.bobo.sort.DocIDPriorityQueue;
import com.browseengine.bobo.sort.SortCollector;
import com.browseengine.bobo.sort.SortCollector.CollectorContext;
import com.browseengine.bobo.util.ListMerger;
import com.senseidb.search.req.ErrorType;
import com.senseidb.search.req.SenseiError;
import com.senseidb.search.req.SenseiHit;
import com.senseidb.search.req.SenseiRequest;
import com.senseidb.search.req.SenseiResult;
import com.senseidb.search.req.mapred.impl.SenseiReduceFunctionWrapper;

public class ResultMerger
{
  private final static Logger logger = Logger.getLogger(ResultMerger.class.getName());

  private final static class MyScoreDoc extends ScoreDoc
  {
    private static final long serialVersionUID = 1L;

    private BoboIndexReader reader;
    private int finalDoc;
    public int groupPos;
    public Object rawGroupValue;
    public Object[] distinctValues;
    public Comparable sortValue;

    public MyScoreDoc(int docid, float score, int finalDoc, BoboIndexReader reader)
    {
      super(docid, score);
      this.finalDoc = finalDoc;
      this.reader = reader;
    }

    SenseiHit getSenseiHit(SenseiRequest req)
    {
      SenseiHit hit = new SenseiHit();
      if (req.isFetchStoredFields() || req.isFetchStoredValue())
      {
        if (req.isFetchStoredFields())
        {
          try
          {
            hit.setStoredFields(reader.document(doc));
          }
          catch(Exception e)
          {
            logger.error(e.getMessage(),e);
          }
        }
        try
        {
          IndexReader innerReader = reader.getInnerReader();
          if (innerReader instanceof ZoieIndexReader)
          {
            hit.setStoredValue(
                ((ZoieIndexReader)innerReader).getStoredValue(
                    ((ZoieIndexReader)innerReader).getUID(doc)));
          }
        }
        catch(Exception e)
        {
        }
      }

      List<FacetHandler<?>> facetHandlers= new ArrayList<FacetHandler<?>>(reader.getFacetHandlerMap().values());
      if (reader.getRuntimeFacetHandlerMap() != null)
      {
        facetHandlers.addAll(reader.getRuntimeFacetHandlerMap().values());
      }
      Map<String,String[]> map = new HashMap<String,String[]>();
      Map<String,Object[]> rawMap = new HashMap<String,Object[]>();
      Set<String> selectSet = req.getSelectSet();
      for (FacetHandler<?> facetHandler : facetHandlers)
      {
        if (selectSet == null ||
            selectSet.size() == 0 ||
            selectSet.contains(facetHandler.getName()))
        {
          map.put(facetHandler.getName(),facetHandler.getFieldValues(reader,doc));
          rawMap.put(facetHandler.getName(),facetHandler.getRawFieldValues(reader,doc));
        }
      }
      hit.setFieldValues(map);
      hit.setRawFieldValues(rawMap);
      hit.setUID(((ZoieIndexReader<BoboIndexReader>)reader.getInnerReader()).getUID(doc));
      hit.setDocid(finalDoc);
      hit.setScore(score);
      hit.setComparable(sortValue);
      hit.setGroupPosition(groupPos);
      String[] groupBy = req.getGroupBy();
      if (groupBy != null && groupBy.length > groupPos && groupBy[groupPos] != null)
      {
        hit.setGroupField(groupBy[groupPos]);
        hit.setGroupValue(hit.getField(groupBy[groupPos]));
        hit.setRawGroupValue(hit.getRawField(groupBy[groupPos]));
      }
      return hit;
    }
  }
  private final static class HitWithGroupQueue
  {
    public SenseiHit hit;
    public DocIDPriorityQueue queue;
    public Map<Object, MyScoreDoc>[] distinctMap;
    public ArrayList<Iterator<SenseiHit>> iterList = new ArrayList<Iterator<SenseiHit>>();

    public HitWithGroupQueue(SenseiHit hit, int maxPerGroup, int distinctLength)
    {
      this.hit = hit;
      this.queue = maxPerGroup <= 1 ? null :
                   new DocIDPriorityQueue(MY_DOC_COMPARATOR,
                                          maxPerGroup,
                                          0);
      this.distinctMap = new Map[distinctLength];
      for (int i = 0; i < distinctLength; ++i)
      {
        this.distinctMap[i] = new HashMap<Object, MyScoreDoc>(maxPerGroup);
      }
    }
  }

  // all sub accessibles are CombinedFacetAccessible
  private final static class MyCombinedFacetAccessible extends CombinedFacetAccessible
  {
    public MyCombinedFacetAccessible(FacetSpec fspec,List<FacetAccessible> list)
    {
      super(fspec, list);
    }

    public int getCappedFacetCount(Object value, int cap) 
    {
      if (_closed)
      {
        throw new IllegalStateException("This instance of count collector was already closed");
      }
      int sum = 0;
      if (_list != null)
      {
        for (FacetAccessible facetAccessor : _list)
        {
          sum += ((CombinedFacetAccessible)facetAccessor).getCappedFacetCount(value, cap-sum);
          if (sum >= cap)
            return cap;
        }
      }
      return sum;
    }
  }

  private final static DocComparator MY_DOC_COMPARATOR = new DocComparator()
  {
    public int compare(ScoreDoc doc1, ScoreDoc doc2)
    {
      return ((MyScoreDoc)doc1).sortValue.compareTo(((MyScoreDoc)doc2).sortValue);
    }

    public Comparable value(ScoreDoc doc)
    {
      return ((MyScoreDoc)doc).sortValue;
    }
  };

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
              count.put(val, oldValue + delta);
            }

          }
        }
        facetAccessible.close();
      }
    }

    Map<String, FacetAccessible> mergedFacetMap = new HashMap<String, FacetAccessible>();
    for (Entry<String,Map<String, Integer>> entry : counts.entrySet())
    {
      String facet = entry.getKey();
      Map<String, Integer> facetValueCounts = entry.getValue();
      List<BrowseFacet> facets = new ArrayList<BrowseFacet>(facetValueCounts.size());
      for (Entry<String, Integer> subEntry : facetValueCounts.entrySet())
      {
        facets.add(new BrowseFacet(subEntry.getKey(), subEntry.getValue()));
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
    for(Entry<String,List<FacetAccessible>> entry : counts.entrySet())
    {
      String fieldname = entry.getKey();
      List<FacetAccessible> facetAccs = entry.getValue();
      if (facetAccs.size() == 1)
      {
        fieldMap.put(fieldname, facetAccs.get(0));
      } else
      {
        fieldMap.put(fieldname, new CombinedFacetAccessible(req.getFacetSpec(fieldname), facetAccs));
      }
    }
    Map<String, FacetAccessible> mergedFacetMap = new HashMap<String, FacetAccessible>();
    for(Entry<String,FacetAccessible> entry : fieldMap.entrySet())
    {
      String fieldname = entry.getKey();
      FacetAccessible facetAcc = entry.getValue();
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
      int ret = f1.getValue().compareTo(f2.getValue());
      if (f1.getValue().startsWith("-") && f2.getValue().startsWith("-")) {
        ret *= -1;
      }
      return ret;
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
    SortField[] _sortFields;

    public SenseiHitComparator(SortField[] sortFields)
    {
      _sortFields = sortFields;
    }

    public int compare(SenseiHit o1, SenseiHit o2)
    {
      if (_sortFields.length == 0)
      {
        return o1.getDocid() - o2.getDocid();
      }
      else
      {
        int equalCount = 0;
        for (int i = 0; i < _sortFields.length; ++i)
        {
          String field = _sortFields[i].getField();
          int reverse = _sortFields[i].getReverse() ? -1 : 1;

          if (_sortFields[i].getType() == SortField.SCORE)
          {
            float score1 = o1.getScore();
            float score2 = o2.getScore();
            if (score1 == score2)
            {
              equalCount++;
              continue;
            }
            else
            {
              return (score1 > score2) ? -reverse : reverse;
            }
          }
          else if (_sortFields[i].getType() == SortField.DOC)
          {
            return o1.getDocid() - o2.getDocid();
          }
          else // A regular sort field
          {
            String value1 = o1.getField(field);
            String value2 = o2.getField(field);

            if (value1 == null && value2 == null)
            {
              equalCount++;
              continue;
            }
            else if (value1 == null)
              return -reverse;
            else if (value2 == null)
              return reverse;
            else
            {
              int comp = value1.compareTo(value2);
              if (value1.startsWith("-") && value2.startsWith("-")) {
                comp *= -1;
              }
              if (comp != 0)
              {
                return comp * reverse;
              }
              else
              {
                equalCount++;
                continue;
              }
            }
          } // A regular sort field
        }

        if (equalCount == _sortFields.length)
        {
          return o1.getDocid() - o2.getDocid();
        }
        else
        {
          return 0;
        }
      }
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

    public int getFacetHitsCount(Object value)
    {
      BrowseFacet facet = _facetMap.get(value);
      if (facet != null)
        return facet.getHitCount();
      return 0;
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

  public static int getNumHits(Collection<SenseiResult> results) {
    int numHits = 0;
    for(SenseiResult res : results)
    {
      numHits += res.getNumHits();
    }
    return numHits;
  }

  public static int getTotalDocs(Collection<SenseiResult> results) {
    int totalDocs = 0;
    for(SenseiResult res : results) {
      totalDocs += res.getTotalDocs();
    }
    return totalDocs;
  }

  public static int getNumGroups(Collection<SenseiResult> results) {
    int numGroups = 0;
    for(SenseiResult res : results) {
      numGroups += res.getNumGroups();
    }
    return numGroups;
  }

  public static long findLongestTime(Collection<SenseiResult> results) {
    long time = 0L;
    for (SenseiResult res : results)
    {
      time = Math.max(time,res.getTime());
    }
    return time;
  }

  public static String findParsedQuery(Collection<SenseiResult> results) {
    for(SenseiResult res : results)
    {
      String parsedQuery = res.getParsedQuery();
      if(parsedQuery != null && parsedQuery.length()>0)
        return parsedQuery;
    }
    return "";
  }

  public static boolean hasSortCollector(Collection<SenseiResult> results) {
    for(SenseiResult res : results)
    {
      if (res.getSortCollector() != null && res.getSortCollector().contextList != null)
      {
        return true;
      }
    }
    return false;
  }

  public static void createUniqueDocIds(Collection<SenseiResult> results) {
    int totalDocs= 0;
    for (SenseiResult res : results)
    {
      SenseiHit[] hits = res.getSenseiHits();
      if (hits != null)
      {
        for (SenseiHit hit : hits)
        {
          hit.setDocid(hit.getDocid() + totalDocs);
        }
      }
      totalDocs += res.getTotalDocs();
    }
  }

  public static List<Iterator<SenseiHit>> flattenHits(Collection<SenseiResult> results) {
    List<Iterator<SenseiHit>> hitList = new ArrayList<Iterator<SenseiHit>>(results.size());

    for (SenseiResult res : results)
    {
      hitList.add(Arrays.asList(res.getSenseiHits()).iterator());
    }
    return hitList;
  }

  private static final int UNKNOWN_GROUP_VALUE_TYPE = 0;
  private static final int NORMAL_GROUP_VALUE_TYPE = 1;
  private static final int LONG_ARRAY_GROUP_VALUE_TYPE = 2;

  public static SenseiResult merge(final SenseiRequest req, Collection<SenseiResult> results, boolean onSearchNode)
  {
    long start = System.currentTimeMillis();
    List<Map<String, FacetAccessible>> facetList = new ArrayList<Map<String, FacetAccessible>>(results.size());

    // Compute the size of hits priority queue
    final int topHits = req.getOffset() + req.getCount();

    // Sum the hits, groups, totalDocs, etc from all the results
    final int numHits = getNumHits(results);
    final int numGroups = getNumGroups(results);
    int totalDocs = getTotalDocs(results);
    final long longestTime = findLongestTime(results);

    final String parsedQuery = findParsedQuery(results);
    final boolean hasSortCollector = hasSortCollector(results);

    // Assign each hit document a unique "document id"
    createUniqueDocIds(results);

    // Extract the hits from the results
    List<Iterator<SenseiHit>> hitLists = flattenHits(results);

    List<FacetAccessible>[] groupAccessibles = extractFacetAccessible(results);

    // Merge your facets
    for (SenseiResult res : results)
    {
      Map<String, FacetAccessible> facetMap = res.getFacetMap();
      if (facetMap != null)
      {
        facetList.add(facetMap);
      }
    }

    Map<String, FacetAccessible> mergedFacetMap = null;
    if (onSearchNode)
    {
      mergedFacetMap = mergeFacetContainerServerSide(facetList, req);
    } else
    {
      mergedFacetMap = mergeFacetContainer(facetList, req);
    }
    Comparator<SenseiHit> comparator = new SenseiHitComparator(req.getSort());

    SenseiHit[] hits;
    if (req.getGroupBy() == null || req.getGroupBy().length == 0)
    {
      List<SenseiHit> mergedList = ListMerger.mergeLists(req.getOffset(), req.getCount(), hitLists
          .toArray(new Iterator[hitLists.size()]), comparator);
      hits = mergedList.toArray(new SenseiHit[mergedList.size()]);
    }
    else
    {
      int[] rawGroupValueType = new int[req.getGroupBy().length];  // 0: unknown, 1: normal, 2: long[]

      PrimitiveLongArrayWrapper primitiveLongArrayWrapperTmp = new PrimitiveLongArrayWrapper(null);

      Iterator<SenseiHit> mergedIter = ListMerger.mergeLists(hitLists, comparator);

      List<SenseiHit> hitsList = null;
      if (!hasSortCollector)
      {
        hitsList = buildHitsListNoSortCollector(req, topHits, rawGroupValueType, mergedIter, req.getOffset());
        //numGroups = (int)(numGroups*(groupHitMap.size()/(float)preGroups));
      }
      else
      {
        int offsetLeft = req.getOffset();

        MyScoreDoc pre = null;

        if (topHits > 0 && groupAccessibles != null && groupAccessibles.length != 0)
        {
          hitsList = buildHitsList(req, results, topHits, groupAccessibles, rawGroupValueType, primitiveLongArrayWrapperTmp);
        }
        else
        {
          hitsList = buildHitsListNoGroupAccessibles(req, topHits, rawGroupValueType, primitiveLongArrayWrapperTmp, mergedIter, offsetLeft);
        }
        //for (int i=0; i<combinedFacetAccessibles.length; ++i) combinedFacetAccessibles[i].close();
      }
      hits = hitsList.toArray(new SenseiHit[hitsList.size()]);

      primitiveLongArrayWrapperTmp = new PrimitiveLongArrayWrapper(null);

      PrepareGroupMappings prepareGroupMappings = new PrepareGroupMappings(req, results, hasSortCollector, hits, rawGroupValueType, primitiveLongArrayWrapperTmp).invoke();
      Map<Object, HitWithGroupQueue>[] groupMaps = prepareGroupMappings.getGroupMaps();
      totalDocs = prepareGroupMappings.getTotalDocs();

      if (hasSortCollector)
      {
        // Fix group position
        for (SenseiHit hit : hits)
        {
          if (hit.getGroupHitsCount() <= 1)
          {
            hit.setGroupPosition(0);
            hit.setGroupField(req.getGroupBy()[0]);
            hit.setGroupValue(hit.getField(req.getGroupBy()[0]));
            hit.setRawGroupValue(hit.getRawField(req.getGroupBy()[0]));
          }
        }

        for (Map<Object, HitWithGroupQueue> map : groupMaps)
        {
          for (HitWithGroupQueue hwg : map.values())
          {
            if (hwg.queue == null) continue;
            int index = hwg.queue.size - 1;
            if (index >= 0)
            {
              SenseiHit[] groupHits = new SenseiHit[index+1];
              while (index >=0)
              {
                groupHits[index] = ((MyScoreDoc)hwg.queue.pop()).getSenseiHit(req);
                --index;
              }
              hwg.hit.setGroupHits(groupHits);
            }
          }
        }
      }
      else
      {
        Set<Object>[] distinctSets = null;
        Object[] distinctValues = null;
        int[] distinctValueType = null;
        if (req.getDistinct() != null && req.getDistinct().length != 0)
        {
          distinctSets = new Set[req.getDistinct().length];
          for (int i = 0; i < distinctSets.length; ++i)
          {
            distinctSets[i] = new HashSet<Object>(req.getMaxPerGroup());
          }
          distinctValues = new Object[distinctSets.length];
          distinctValueType = new int[distinctSets.length];
          primitiveLongArrayWrapperTmp = new PrimitiveLongArrayWrapper(null);
        }
        for (Map<Object, HitWithGroupQueue> map : groupMaps)
        {
          for (HitWithGroupQueue hwg : map.values())
          {
            if (distinctSets != null)
            {
              for (int i = 0; i < distinctSets.length; ++i)
              {
                distinctSets[i].clear();
              }
            }
            Iterator<SenseiHit> groupMergedIter = ListMerger.mergeLists(hwg.iterList
                .toArray(new Iterator[hwg.iterList.size()]), comparator);
            List<SenseiHit> mergedList = new ArrayList<SenseiHit>(req.getMaxPerGroup());
            while (mergedList.size() < req.getMaxPerGroup() && groupMergedIter.hasNext())
            {
              SenseiHit hit = groupMergedIter.next();
              if (distinctSets == null || !checkDuplicate(hit,
                                                          distinctSets,
                                                          req.getDistinct(),
                                                          distinctValues,
                                                          distinctValueType,
                                                          primitiveLongArrayWrapperTmp))
              {
                mergedList.add(hit);
              }
            }
            SenseiHit[] groupHits = mergedList.toArray(new SenseiHit[mergedList.size()]);
            hwg.hit.setGroupHits(groupHits);
          }
        }
      }
    }

    if (groupAccessibles != null)
    {
      for (List<FacetAccessible> list : groupAccessibles)
      {
        if (list != null)
        {
          for (FacetAccessible acc : list)
          {
            if (acc != null)
              acc.close();
          }
        }
      }
    }

    SenseiResult merged = new SenseiResult();
    merged.setHits(hits);
    merged.setNumHits(numHits);
    merged.setNumGroups(numGroups);
    merged.setTotalDocs(totalDocs);
    merged.addAll(mergedFacetMap);

    long end = System.currentTimeMillis();

    merged.setTime(longestTime + end - start);
    mergerErrors(merged, req, results, parsedQuery);
    if (req.getMapReduceFunction() != null) {
      if (onSearchNode) {
        merged.setMapReduceResult(SenseiReduceFunctionWrapper.combine(req.getMapReduceFunction(), SenseiReduceFunctionWrapper.extractMapReduceResults(results)));
      } else {
        //on broker level
        merged.setMapReduceResult(SenseiReduceFunctionWrapper.reduce(req.getMapReduceFunction(), SenseiReduceFunctionWrapper.extractMapReduceResults(results)));
      }
    }
    return merged;
  }

  private static boolean checkDuplicate(SenseiHit hit,
                                        Set<Object>[] distinctSets,
                                        String[] distinct,
                                        Object[] distinctValues,
                                        int[] distinctValueType,
                                        PrimitiveLongArrayWrapper primitiveLongArrayWrapperTmp)
  {
    for (int i = 0; i < distinctValues.length; ++i)
    {
      distinctValues[i] = hit.getRawField(distinct[i]);
      distinctValues[i] = extractRawGroupValue(distinctValueType, i,
          primitiveLongArrayWrapperTmp, distinctValues[i]);

      if (distinctSets[i].contains(distinctValues[i])) return true;
    }
    for (int i = 0; i < distinctValues.length; ++i)
    {
      if (distinctValueType[i] == LONG_ARRAY_GROUP_VALUE_TYPE)
        distinctSets[i].add(new PrimitiveLongArrayWrapper(((PrimitiveLongArrayWrapper)distinctValues[i]).data));
      else
        distinctSets[i].add(distinctValues[i]);
    }
    return false;
  }

  private static List<SenseiHit> buildHitsListNoGroupAccessibles(SenseiRequest req,
                                                                 int topHits,
                                                                 int[] rawGroupValueType,
                                                                 PrimitiveLongArrayWrapper primitiveLongArrayWrapperTmp,
                                                                 Iterator<SenseiHit> mergedIter,
                                                                 int offsetLeft) {
    List<SenseiHit> hitsList = new ArrayList<SenseiHit>(req.getCount());

    Object rawGroupValue = null;
    Object firstRawGroupValue = null;
    Set<Object>[] groupSets = new Set[1];
    groupSets[0] = new HashSet<Object>(topHits);
    while(mergedIter.hasNext())
    {

      SenseiHit hit = mergedIter.next();
      firstRawGroupValue = null;
      int i=0;
      for (; i<groupSets.length; ++i)
      {
        //rawGroupValue = hit.getRawField(req.getGroupBy()[i]);

        rawGroupValue = extractRawGroupValue(rawGroupValueType, i,
            primitiveLongArrayWrapperTmp, hit);

        if (firstRawGroupValue == null) firstRawGroupValue = rawGroupValue;
        if (groupSets[i].contains(rawGroupValue))
        {
          i = -1;
          break;
        }
      }
      if (i >= 0)
      {
        if (i >= groupSets.length)
        {
          i = 0;
          rawGroupValue = firstRawGroupValue;
        }
        if (offsetLeft > 0)
          --offsetLeft;
        else {
          //hit.setGroupHitsCount(combinedFacetAccessibles[i].getFacetHitsCount(hit.getRawGroupValue()));
          hitsList.add(hit);
          if (hitsList.size() >= req.getCount())
            break;
        }
        if (rawGroupValueType[i] == LONG_ARRAY_GROUP_VALUE_TYPE)
          groupSets[i].add(new PrimitiveLongArrayWrapper(primitiveLongArrayWrapperTmp.data));
        else
          groupSets[i].add(rawGroupValue);
      }
    }

    return hitsList;
  }

  private static List<SenseiHit> buildHitsList(SenseiRequest req,
                                               Collection<SenseiResult> results,
                                               int topHits,
                                               List<FacetAccessible>[] groupAccessibles,
                                               int[] rawGroupValueType,
                                               PrimitiveLongArrayWrapper primitiveLongArrayWrapperTmp) {
    List<SenseiHit> hitsList = new ArrayList<SenseiHit>(req.getCount());

    MyScoreDoc pre = null;
    Object rawGroupValue = null;
    MyCombinedFacetAccessible[] combinedFacetAccessibles = new MyCombinedFacetAccessible[groupAccessibles.length];
    for(int i = 0; i < groupAccessibles.length; i++)
    {
      combinedFacetAccessibles[i] = new MyCombinedFacetAccessible(new FacetSpec(), groupAccessibles[i]);
    }

    Set<Object>[] groupSets = new Set[rawGroupValueType.length];
    for (int i = 0; i < rawGroupValueType.length; ++i)
    {
      groupSets[i] = new HashSet<Object>(topHits);
    }

    Map<Object, MyScoreDoc>[] valueDocMaps = new Map[rawGroupValueType.length];
    for (int i = 0; i < rawGroupValueType.length; ++i)
    {
      valueDocMaps[i] = new HashMap<Object, MyScoreDoc>(topHits);
    }

    int totalDocs = 0;

    MyScoreDoc tmpScoreDoc = new MyScoreDoc(0, 0.0f, 0, null);

    MyScoreDoc bottom = null;
    boolean queueFull = false;

    DocIDPriorityQueue docQueue = new DocIDPriorityQueue(MY_DOC_COMPARATOR, topHits, 0);

    for (SenseiResult res : results)
    {
      SortCollector sortCollector = res.getSortCollector();
      if (sortCollector == null)
        continue;

      int end = (res.getNumHits() % SortCollector.BLOCK_SIZE) - 1;

      Iterator<CollectorContext> contextIter = sortCollector.contextList.descendingIterator();

      // Populate dataCaches and contextLeft
      CollectorContext currentContext = null;
      int contextLeft = 0;
      FacetDataCache[] dataCaches = new FacetDataCache[sortCollector.groupByMulti.length];
      while (contextIter.hasNext()) {
        currentContext = contextIter.next();
        currentContext.restoreRuntimeFacets();
        contextLeft = currentContext.length;
        if (contextLeft > 0)
        {
          for (int j=0; j<sortCollector.groupByMulti.length; ++j)
            dataCaches[j] = (FacetDataCache)sortCollector.groupByMulti[j].getFacetData(currentContext.reader);
          break;
        }
      }

      Iterator<float[]> scoreArrayIter = sortCollector.scorearraylist != null ? sortCollector.scorearraylist.descendingIterator():null;

      if (contextLeft > 0)
      {
        Iterator<int[]> docArrayIter = sortCollector.docidarraylist.descendingIterator();
        while (docArrayIter.hasNext())
        {
          int[] docs = docArrayIter.next();
          float[] scores = scoreArrayIter != null ? scoreArrayIter.next():null;

          for (int i = end; i >= 0; --i)
          {
            tmpScoreDoc.doc = docs[i];
            tmpScoreDoc.score = scores != null ? scores[i] : 0.0f;
            tmpScoreDoc.finalDoc = currentContext.base + totalDocs + tmpScoreDoc.doc;
            tmpScoreDoc.reader = currentContext.reader;
            tmpScoreDoc.sortValue = currentContext.comparator.value(tmpScoreDoc);

            int j=0;

            if (!queueFull || tmpScoreDoc.sortValue.compareTo(bottom.sortValue) < 0)
            {
              for (;; ++j)
              {
                rawGroupValue = dataCaches[j].valArray.getRawValue(dataCaches[j].orderArray.get(tmpScoreDoc.doc));

                rawGroupValue = extractRawGroupValue(rawGroupValueType, j,
                    primitiveLongArrayWrapperTmp, rawGroupValue);

                pre = valueDocMaps[j].get(rawGroupValue);
                if (pre != null)
                {
                  j = -1;
                  break;
                }

                if (j >= combinedFacetAccessibles.length) break;

                if (rawGroupValueType[j] == LONG_ARRAY_GROUP_VALUE_TYPE)
                {
                  if (combinedFacetAccessibles[j].getCappedFacetCount(primitiveLongArrayWrapperTmp.data, 2) != 1)
                    break;
                }
                else
                {
                  if (combinedFacetAccessibles[j].getCappedFacetCount(rawGroupValue, 2) != 1)
                    break;
                }
              }
              if (j < 0)
              {
                if (tmpScoreDoc.sortValue.compareTo(pre.sortValue) < 0)
                {
                  tmpScoreDoc.groupPos = pre.groupPos;
                  tmpScoreDoc.rawGroupValue = pre.rawGroupValue;

                  // Pre has a higher score. Pop it in the queue!
                  bottom = (MyScoreDoc)docQueue.replace(tmpScoreDoc, pre);
                  valueDocMaps[tmpScoreDoc.groupPos].put(tmpScoreDoc.rawGroupValue, tmpScoreDoc);
                  tmpScoreDoc = pre;
                }
              }
              else
              {
                if (queueFull)
                {
                  tmpScoreDoc.groupPos = j;
                  tmpScoreDoc.rawGroupValue = rawGroupValue;
                  MyScoreDoc tmp = bottom;

                  valueDocMaps[tmp.groupPos].remove(tmp.rawGroupValue);

                  bottom = (MyScoreDoc)docQueue.replace(tmpScoreDoc);
                  valueDocMaps[j].put(rawGroupValue, tmpScoreDoc);
                  tmpScoreDoc = tmp;
                  if (rawGroupValueType[tmpScoreDoc.groupPos] == LONG_ARRAY_GROUP_VALUE_TYPE)
                  {
                    primitiveLongArrayWrapperTmp = (PrimitiveLongArrayWrapper)tmpScoreDoc.rawGroupValue;
                  }
                  else
                  {
                    primitiveLongArrayWrapperTmp = new PrimitiveLongArrayWrapper(null);
                  }
                }
                else
                {
                  MyScoreDoc tmp = new MyScoreDoc(tmpScoreDoc.doc, tmpScoreDoc.score, currentContext.base + totalDocs + tmpScoreDoc.doc, currentContext.reader);
                  tmp.groupPos = j;
                  tmp.rawGroupValue = rawGroupValue;
                  tmp.sortValue = tmpScoreDoc.sortValue;
                  bottom = (MyScoreDoc)docQueue.add(tmp);
                  valueDocMaps[j].put(rawGroupValue, tmp);
                  queueFull = (docQueue.size >= topHits);
                  if (rawGroupValueType[j] == LONG_ARRAY_GROUP_VALUE_TYPE)
                  {
                    primitiveLongArrayWrapperTmp = new PrimitiveLongArrayWrapper(null);
                  }
                }
              }
            }

            --contextLeft;
            if (contextLeft <= 0)
            {
              while (contextIter.hasNext())
              {
                currentContext = contextIter.next();
                currentContext.restoreRuntimeFacets();
                contextLeft = currentContext.length;
                if (contextLeft > 0)
                {
                  for (j=0; j<sortCollector.groupByMulti.length; ++j)
                    dataCaches[j] = (FacetDataCache)sortCollector.groupByMulti[j].getFacetData(currentContext.reader);
                  break;
                }
              }
              if (contextLeft <= 0) // No more docs left.
                break;
            }
          }
          end = SortCollector.BLOCK_SIZE - 1;
        }
      }
      totalDocs += res.getTotalDocs();
    }


    int len = docQueue.size() - req.getOffset();
    if (len < 0) len = 0;
    SenseiHit[] hitArray = new SenseiHit[len];
    for (int i = hitArray.length-1; i>=0; --i)
    {
      tmpScoreDoc = (MyScoreDoc)docQueue.pop();
      hitArray[i] = tmpScoreDoc.getSenseiHit(req);
    }

    for (int i=0; i<hitArray.length; ++i)
      hitsList.add(hitArray[i]);

    return hitsList;
  }

  private static List<SenseiHit> buildHitsListNoSortCollector(SenseiRequest req,
                                                              int topHits,
                                                              int[] rawGroupValueType,
                                                              Iterator<SenseiHit> mergedIter,
                                                              int offsetLeft) {

    List<SenseiHit> hitsList = new ArrayList<SenseiHit>(req.getCount());

    // TODO: Pull out the sensei hits extraction from this function
    PrimitiveLongArrayWrapper primitiveLongArrayWrapperTmp = new PrimitiveLongArrayWrapper(null);

    Map<Object, SenseiHit>[] groupHitMaps = new Map[req.getGroupBy().length];
    for (int i=0; i < groupHitMaps.length; ++i)
    {
      groupHitMaps[i] = new HashMap<Object, SenseiHit>(topHits);
    }

    while(mergedIter.hasNext())
    {
      SenseiHit hit = mergedIter.next();
      Object rawGroupValue = extractRawGroupValue(rawGroupValueType, hit.getGroupPosition(), primitiveLongArrayWrapperTmp, hit);

      SenseiHit pre = groupHitMaps[hit.getGroupPosition()].get(rawGroupValue);
      if (pre != null)
      {
        if (offsetLeft <= 0) {
          pre.setGroupHitsCount(pre.getGroupHitsCount()+hit.getGroupHitsCount());
        }
      }
      else
      {
        if (offsetLeft > 0)
          --offsetLeft;
        else if (hitsList.size() < req.getCount())
          hitsList.add(hit);

        if (rawGroupValueType[0] == 2)
          groupHitMaps[hit.getGroupPosition()].put(new PrimitiveLongArrayWrapper(primitiveLongArrayWrapperTmp.data), hit);
        else
          groupHitMaps[hit.getGroupPosition()].put(rawGroupValue, hit);
      }
    }
    return hitsList;
  }

  private static Object extractRawGroupValue(int[] rawGroupValueType, int groupPosition,
                                             PrimitiveLongArrayWrapper primitiveLongArrayWrapperTmp, SenseiHit hit) {
    return extractRawGroupValue(rawGroupValueType, groupPosition, primitiveLongArrayWrapperTmp, hit.getRawGroupValue());
  }

  private static Object extractRawGroupValue(int[] rawGroupValueType, int groupPosition,
                                             PrimitiveLongArrayWrapper primitiveLongArrayWrapperTmp, Object rawGroupValue) {

    if (rawGroupValueType[groupPosition] == LONG_ARRAY_GROUP_VALUE_TYPE)
    {
      // We already know this group position is a long[]
      primitiveLongArrayWrapperTmp.data = (long[])rawGroupValue;
      rawGroupValue = primitiveLongArrayWrapperTmp;
    }
    else if (rawGroupValueType[groupPosition] == UNKNOWN_GROUP_VALUE_TYPE)
    {
      // Unknown
      if (rawGroupValue != null)
      {
        if (rawGroupValue instanceof long[])
        {
          // It's a long array, so set the position
          rawGroupValueType[groupPosition] = LONG_ARRAY_GROUP_VALUE_TYPE;
          primitiveLongArrayWrapperTmp.data = (long[])rawGroupValue;
          rawGroupValue = primitiveLongArrayWrapperTmp;
        }
        else
          rawGroupValueType[groupPosition] = NORMAL_GROUP_VALUE_TYPE;
      }
    }
    return rawGroupValue;
  }

  private static List<FacetAccessible>[] extractFacetAccessible(Collection<SenseiResult> results) {
    List<FacetAccessible>[] groupAccessibles = null;
    for (SenseiResult res : results)
    {
      if (res.getGroupAccessibles() != null)
      {
        if (groupAccessibles == null)
        {
          groupAccessibles = new List[res.getGroupAccessibles().length];
          for (int i=0; i<groupAccessibles.length; ++i)
          {
            groupAccessibles[i] = new ArrayList<FacetAccessible>(results.size());
          }
        }
        for (int i=0; i<groupAccessibles.length; ++i)
        {
          groupAccessibles[i].add(res.getGroupAccessibles()[i]);
        }
      }
    }
    return groupAccessibles;
  }

  public static class PrepareGroupMappings {
    private final SenseiRequest req;
    private final int maxPerGroup;
    private final int distinctLength;
    private final Collection<SenseiResult> results;
    private final boolean hasSortCollector;
    private final SenseiHit[] hits;
    private final int[] rawGroupValueType;
    private final PrimitiveLongArrayWrapper primitiveLongArrayWrapperTmp;
    private final int[] distinctValueType;
    private final PrimitiveLongArrayWrapper distinctPrimitiveLongArrayWrapperTmp;

    private int totalDocs;
    private Map<Object, HitWithGroupQueue>[] groupMaps;

    public PrepareGroupMappings(SenseiRequest req,
                                Collection<SenseiResult> results,
                                boolean hasSortCollector,
                                SenseiHit[] hits,
                                int[] rawGroupValueType,
                                PrimitiveLongArrayWrapper primitiveLongArrayWrapperTmp) {
      this.req = req;
      this.maxPerGroup = req.getMaxPerGroup() <= 1 ? 0 : req.getMaxPerGroup();
      this.distinctLength = req.getDistinct() == null ? 0 : req.getDistinct().length;
      this.results = results;
      this.hasSortCollector = hasSortCollector;
      this.hits = hits;
      this.rawGroupValueType = rawGroupValueType;
      this.primitiveLongArrayWrapperTmp = primitiveLongArrayWrapperTmp;
      this.distinctValueType = new int[distinctLength];
      this.distinctPrimitiveLongArrayWrapperTmp = new PrimitiveLongArrayWrapper(null);

      groupMaps = new Map[req.getGroupBy().length];
      for (int i=0; i< groupMaps.length; ++i)
      {
        groupMaps[i] = new HashMap<Object, HitWithGroupQueue>(hits.length*2);
      }
    }

    public int getTotalDocs() {
      return totalDocs;
    }

    public Map<Object, HitWithGroupQueue>[] getGroupMaps() {
      return groupMaps;
    }

    public PrepareGroupMappings invoke() {
      Object rawGroupValue;

      for (SenseiHit hit : hits)
      {
        rawGroupValue = hit.getRawField(req.getGroupBy()[hit.getGroupPosition()]);

        rawGroupValue = extractRawGroupValue(rawGroupValueType, hit.getGroupPosition(),
            primitiveLongArrayWrapperTmp, rawGroupValue);

        if (rawGroupValueType[hit.getGroupPosition()] == LONG_ARRAY_GROUP_VALUE_TYPE)
        {
          groupMaps[hit.getGroupPosition()].put(new PrimitiveLongArrayWrapper(primitiveLongArrayWrapperTmp.data),
                                                new HitWithGroupQueue(hit,
                                                                      maxPerGroup,
                                                                      distinctLength));
        }
        else
        {
          groupMaps[hit.getGroupPosition()].put(rawGroupValue,
                                                new HitWithGroupQueue(hit,
                                                                      maxPerGroup,
                                                                      distinctLength));
        }
      }

      MyScoreDoc tmpScoreDoc = null, pre = null;
      int doc = 0;
      float score = 0.0f;
      int i = 0, j = 0, k = 0;
      HitWithGroupQueue hitWithGroupQueue = null;

      totalDocs = 0;
      for (SenseiResult res : results)
      {
        if (hasSortCollector)
        {
          SortCollector sortCollector = res.getSortCollector();
          if (sortCollector == null) continue;
          int end = (res.getNumHits() % SortCollector.BLOCK_SIZE) - 1;
          Iterator<CollectorContext> contextIter = sortCollector.contextList.descendingIterator();
          CollectorContext currentContext = null;
          int contextLeft = 0;
          FacetDataCache[] dataCaches = new FacetDataCache[sortCollector.groupByMulti.length];
          FacetDataCache[] distinctDataCaches = new FacetDataCache[distinctLength];
          while (contextIter.hasNext()) {
            currentContext = contextIter.next();
            currentContext.restoreRuntimeFacets();
            contextLeft = currentContext.length;
            if (contextLeft > 0)
            {
              for (j=0; j<sortCollector.groupByMulti.length; ++j)
              {
                dataCaches[j] = (FacetDataCache)sortCollector.groupByMulti[j].getFacetData(currentContext.reader);
              }
              for (j=0; j<distinctLength; ++j)
              {
                distinctDataCaches[j] = (FacetDataCache)currentContext.
                                                        reader.
                                                        getFacetHandler(req.getDistinct()[j]).
                                                        getFacetData(currentContext.reader);
              }
              break;
            }
          }

          Iterator<float[]> scoreArrayIter = sortCollector.scorearraylist != null ? sortCollector.scorearraylist.descendingIterator():null;
          if (contextLeft > 0)
          {
            Iterator<int[]> docArrayIter = sortCollector.docidarraylist.descendingIterator();
            while (docArrayIter.hasNext())
            {
              int[] docs = docArrayIter.next();
              float[] scores = scoreArrayIter != null ? scoreArrayIter.next():null;
              for (i = end; i >= 0; --i)
              {
                doc = docs[i];
                score = scores != null ? scores[i]:0.0f;
                for (j=0; j<sortCollector.groupByMulti.length; ++j)
                {
                  rawGroupValue = extractRawGroupValue(rawGroupValueType, j, primitiveLongArrayWrapperTmp,
                      dataCaches[j].valArray.getRawValue(dataCaches[j].orderArray.get(doc)));

                  hitWithGroupQueue = groupMaps[j].get(rawGroupValue);
                  if (hitWithGroupQueue != null)
                  {
                    hitWithGroupQueue.hit.setGroupHitsCount(hitWithGroupQueue.hit.getGroupHitsCount() + 1);
                    if (hitWithGroupQueue.queue == null) break;
                    // Collect this hit.
                    if (tmpScoreDoc == null)
                      tmpScoreDoc = new MyScoreDoc(doc, score, currentContext.base + totalDocs + doc, currentContext.reader);
                    else
                    {
                      tmpScoreDoc.doc = doc;
                      tmpScoreDoc.score = score;
                      tmpScoreDoc.finalDoc = currentContext.base + totalDocs + doc;
                      tmpScoreDoc.reader = currentContext.reader;
                    }
                    tmpScoreDoc.sortValue = currentContext.comparator.value(tmpScoreDoc);
                    tmpScoreDoc.groupPos = j;
                    if (distinctLength > 0)
                    {
                      tmpScoreDoc.distinctValues = new Object[distinctLength];
                    }

                    if (hitWithGroupQueue.queue.size < maxPerGroup ||
                        tmpScoreDoc.sortValue.compareTo(((MyScoreDoc)hitWithGroupQueue.queue.top()).sortValue) < 0)
                    {
                      pre = null;
                      for (k = 0; k < distinctLength; ++k)
                      {
                        tmpScoreDoc.distinctValues[k] = extractRawGroupValue(distinctValueType,
                                                                             k,
                                                                             distinctPrimitiveLongArrayWrapperTmp,
                                                                             distinctDataCaches[k].valArray.getRawValue(
                                                                               distinctDataCaches[k].orderArray.get(doc)
                                                                             ));
                        if (pre == null)
                          pre = hitWithGroupQueue.distinctMap[k].get(tmpScoreDoc.distinctValues[k]);
                      }
                      if (pre != null)
                      {
                        if (tmpScoreDoc.sortValue.compareTo(pre.sortValue) < 0)
                        {
                          hitWithGroupQueue.queue.replace(tmpScoreDoc, pre);
                          for (k = 0; k < distinctLength; ++k)
                          {
                            hitWithGroupQueue.distinctMap[k].remove(pre.distinctValues[k]);
                            hitWithGroupQueue.distinctMap[k].put(tmpScoreDoc.distinctValues[k], tmpScoreDoc);
                          }
                          tmpScoreDoc = pre;
                        }
                      }
                      else
                      {
                        if (hitWithGroupQueue.queue.size >= maxPerGroup) // queue full
                        {
                          pre = (MyScoreDoc)hitWithGroupQueue.queue.top();
                          hitWithGroupQueue.queue.replace(tmpScoreDoc);
                          for (k = 0; k < distinctLength; ++k)
                          {
                            hitWithGroupQueue.distinctMap[k].remove(pre.distinctValues[k]);
                            hitWithGroupQueue.distinctMap[k].put(tmpScoreDoc.distinctValues[k], tmpScoreDoc);
                          }
                          tmpScoreDoc = pre;
                        }
                        else
                        {
                          hitWithGroupQueue.queue.add(tmpScoreDoc);
                          for (k = 0; k < distinctLength; ++k)
                          {
                            hitWithGroupQueue.distinctMap[k].put(tmpScoreDoc.distinctValues[k], tmpScoreDoc);
                          }
                          tmpScoreDoc = null;
                        }
                      }
                    }
                    break;
                  }
                }
                --contextLeft;
                if (contextLeft <= 0)
                {
                  while (contextIter.hasNext()) {
                    currentContext = contextIter.next();
                    currentContext.restoreRuntimeFacets();
                    contextLeft = currentContext.length;
                    if (contextLeft > 0)
                    {
                      for (j=0; j<sortCollector.groupByMulti.length; ++j)
                      {
                        dataCaches[j] = (FacetDataCache)sortCollector.groupByMulti[j].getFacetData(currentContext.reader);
                      }
                      for (j=0; j<distinctLength; ++j)
                      {
                        distinctDataCaches[j] = (FacetDataCache)currentContext.
                                                                reader.
                                                                getFacetHandler(req.getDistinct()[j]).
                                                                getFacetData(currentContext.reader);
                      }
                      break;
                    }
                  }
                  if (contextLeft <= 0) // No more docs left.
                    break;
                }
              }
              end = SortCollector.BLOCK_SIZE - 1;
            }
          }
        }
        else
        {
          if (res.getSenseiHits() != null)
          {
            for (SenseiHit hit : res.getSenseiHits())
            {
              if (hit.getGroupHits() != null)
              {
                rawGroupValue = hit.getRawGroupValue();
                if (rawGroupValueType[hit.getGroupPosition()] == LONG_ARRAY_GROUP_VALUE_TYPE)
                {
                  primitiveLongArrayWrapperTmp.data = (long[])rawGroupValue;
                  rawGroupValue = primitiveLongArrayWrapperTmp;
                }

                hitWithGroupQueue = groupMaps[hit.getGroupPosition()].get(rawGroupValue);
                if (hitWithGroupQueue != null)
                  hitWithGroupQueue.iterList.add(Arrays.asList(hit.getSenseiGroupHits()).iterator());
              }
            }
          }
        }
        totalDocs += res.getTotalDocs();
      }
      return this;
    }
  }
  private static void mergerErrors(SenseiResult merged, final SenseiRequest req, Collection<SenseiResult> results, String parsedQuery) {
    merged.setParsedQuery(parsedQuery);
    merged.getErrors().addAll(req.getErrors());
    for (SenseiResult res : results) {
      merged.getErrors().addAll(res.getErrors());
      if (res.getBoboErrors().size() > 0) {
        for (String boboError : res.getBoboErrors()) {
          merged.addError(new SenseiError(boboError, ErrorType.BoboExecutionError));
        }
      }
    }
  }
}
