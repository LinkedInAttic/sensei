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
import com.browseengine.bobo.facets.data.PrimitiveLongArrayWrapper;
import com.browseengine.bobo.facets.data.DeepObjectArrayWrapper;
import com.browseengine.bobo.sort.DocComparator;
import com.browseengine.bobo.sort.DocIDPriorityQueue;
import com.browseengine.bobo.sort.SortCollector;
import com.browseengine.bobo.sort.SortCollector.CollectorContext;
import com.browseengine.bobo.util.ListMerger;
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
    public Comparable sortValue;

    public MyScoreDoc(int docid, float score, int finalDoc, BoboIndexReader reader)
    {
      super(docid, score);
      this.finalDoc = finalDoc;
      this.reader = reader;
    }

    SenseiHit getSenseiHit(boolean fetchStoredFields, boolean fetchStoredValue)
    {
      SenseiHit hit = new SenseiHit();
      if (fetchStoredFields || fetchStoredValue)
      {
        if (fetchStoredFields)
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

      Collection<FacetHandler<?>> facetHandlers= reader.getFacetHandlerMap().values();
      Map<String,String[]> map = new HashMap<String,String[]>();
      Map<String,Object[]> rawMap = new HashMap<String,Object[]>();
      for (FacetHandler<?> facetHandler : facetHandlers)
      {
          map.put(facetHandler.getName(),facetHandler.getFieldValues(reader,doc));
          rawMap.put(facetHandler.getName(),facetHandler.getRawFieldValues(reader,doc));
      }
      hit.setFieldValues(map);
      hit.setRawFieldValues(rawMap);
      hit.setUID(((ZoieIndexReader<BoboIndexReader>)reader.getInnerReader()).getUID(doc));
      hit.setDocid(finalDoc);
      hit.setScore(score);
      hit.setComparable(sortValue);
      hit.setGroupPosition(groupPos);
      return hit;
    }
  }
  private final static class HitWithGroupQueue
  {
    public SenseiHit hit;
    public PriorityQueue<MyScoreDoc> queue;
    public ArrayList<Iterator<SenseiHit>> iterList = new ArrayList<Iterator<SenseiHit>>();

    public HitWithGroupQueue(SenseiHit hit, PriorityQueue<MyScoreDoc> queue)
    {
      this.hit = hit;
      this.queue = queue;
    }
  }
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

  public static SenseiResult merge(final SenseiRequest req, Collection<SenseiResult> results, boolean onSearchNode)
  {
    long start = System.currentTimeMillis();
    List<Map<String, FacetAccessible>> facetList = new ArrayList<Map<String, FacetAccessible>>(results.size());

    ArrayList<Iterator<SenseiHit>> iteratorList = new ArrayList<Iterator<SenseiHit>>(results.size());
    int numHits = 0;
    //int preGroups = 0;
    int numGroups = 0;
    int totalDocs = 0;

    long time = 0L;
    
    List<FacetAccessible>[] groupAccessibles = null;
    
    String parsedQuery = null;
    boolean hasSortCollector = false;
    for (SenseiResult res : results)
    {
      if (res.getSortCollector() != null) hasSortCollector = true;

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
      numGroups += res.getNumGroups();
      totalDocs += res.getTotalDocs();
      time = Math.max(time,res.getTime());
      Map<String, FacetAccessible> facetMap = res.getFacetMap();
      if (facetMap != null)
      {
        facetList.add(facetMap);
      }
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
    Comparator<SenseiHit> comparator = new SenseiHitComparator(req.getSort());

    SenseiHit[] hits;
    if (req.getGroupBy() == null || req.getGroupBy().length == 0)
    {
      List<SenseiHit> mergedList = ListMerger.mergeLists(req.getOffset(), req.getCount(), iteratorList
          .toArray(new Iterator[iteratorList.size()]), comparator);
      hits = mergedList.toArray(new SenseiHit[mergedList.size()]);
    }
    else {
      int[] rawGroupValueType = new int[req.getGroupBy().length];  // 0: unknown, 1: normal, 2: long[]

      PrimitiveLongArrayWrapper primitiveLongArrayWrapperTmp = new PrimitiveLongArrayWrapper(null);

      Object rawGroupValue = null;
      Object firstRawGroupValue = null;

      List<SenseiHit> hitsList = new ArrayList<SenseiHit>(req.getCount());
      Iterator<SenseiHit> mergedIter = ListMerger.mergeLists(iteratorList, comparator);
      int offsetLeft = req.getOffset();
      if (groupAccessibles == null)
      {
        Map<Object, SenseiHit> groupHitMap = new HashMap<Object, SenseiHit>(req.getCount());
        while(mergedIter.hasNext())
        {
          //++preGroups;
          SenseiHit hit = mergedIter.next();
          rawGroupValue = hit.getRawGroupValue();
          if (rawGroupValueType[0] == 0) {
            if (rawGroupValue != null)
            {
              if (rawGroupValue instanceof long[])
                rawGroupValueType[0] = 2;
              else
                rawGroupValueType[0] = 1;
            }
          }
          if (rawGroupValueType[0] == 2)
          {
            primitiveLongArrayWrapperTmp.data = (long[])rawGroupValue;
            rawGroupValue = primitiveLongArrayWrapperTmp;
          }

          SenseiHit pre = groupHitMap.get(rawGroupValue);
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
            else if (hitsList.size()<req.getCount())
              hitsList.add(hit);

            if (rawGroupValueType[0] == 2)
              groupHitMap.put(new PrimitiveLongArrayWrapper(primitiveLongArrayWrapperTmp.data), hit);
            else
              groupHitMap.put(rawGroupValue, hit);
          }
        }
        //numGroups = (int)(numGroups*(groupHitMap.size()/(float)preGroups));
      }
      else
      {
        FacetAccessible[] combinedFacetAccessibles = new FacetAccessible[groupAccessibles.length];
        Set<Object>[] groupSets = new Set[groupAccessibles.length];
        for (int i=0; i<groupAccessibles.length; ++i)
        {
          combinedFacetAccessibles[i] = new CombinedFacetAccessible(new FacetSpec(), groupAccessibles[i]);
          groupSets[i] = new HashSet<Object>(req.getCount());
        }
        MyScoreDoc pre = null;
        int topHits = req.getOffset() + req.getCount();
        if (topHits > 0 && combinedFacetAccessibles.length > 1 && hasSortCollector)
        {
          totalDocs = 0;
          Object[] vals = null;
          MyScoreDoc tmpScoreDoc = new MyScoreDoc(0, 0.0f, 0, null);
          MyScoreDoc bottom = null;
          boolean queueFull = false;
          Map<Object, MyScoreDoc>[] valueDocMaps = new Map[combinedFacetAccessibles.length];
          for (int i=0; i<valueDocMaps.length; ++i)
          {
            valueDocMaps[i] = new HashMap<Object, MyScoreDoc>(topHits);
          }
          DocIDPriorityQueue docQueue = new DocIDPriorityQueue(new DocComparator()
            {
              public int compare(ScoreDoc doc1, ScoreDoc doc2)
              {
                return ((MyScoreDoc)doc1).sortValue.compareTo(((MyScoreDoc)doc2).sortValue);
              }

              public Comparable value(ScoreDoc doc)
              {
                return ((MyScoreDoc)doc).sortValue;
              }
            }, topHits, 0);

          for (SenseiResult res : results)
          {
            SortCollector sortCollector = res.getSortCollector();
            Iterator<CollectorContext> contextIter = sortCollector.contextList.iterator();
            CollectorContext currentContext = null;
            int contextLeft = 0;
            while (contextIter.hasNext()) {
              currentContext = contextIter.next();
              contextLeft = currentContext.length;
              if (contextLeft > 0)
                break;
            }

            Iterator<float[]> scoreArrayIter = sortCollector.scorearraylist != null ? sortCollector.scorearraylist.iterator():null;
            if (contextLeft > 0)
            {
              for (int[] docs : sortCollector.docidarraylist)
              {
                float[] scores = scoreArrayIter != null ? scoreArrayIter.next():null;
                for (int i=0; i<SortCollector.BLOCK_SIZE; ++i)
                {
                  tmpScoreDoc.doc = docs[i];
                  tmpScoreDoc.score = scores != null ? scores[i]:0.0f;
                  tmpScoreDoc.finalDoc = currentContext.base + totalDocs + tmpScoreDoc.doc;
                  tmpScoreDoc.reader = currentContext.reader;
                  tmpScoreDoc.sortValue = currentContext.comparator.value(tmpScoreDoc);

                  firstRawGroupValue = null;
                  int j=0;
                  for (; j<sortCollector.groupByMulti.length; ++j)
                  {
                    vals = sortCollector.groupByMulti[j].getRawFieldValues(currentContext.reader, tmpScoreDoc.doc);
                    if (vals != null && vals.length > 0)
                      rawGroupValue = vals[0];
                    else
                      rawGroupValue = null;
                    if (rawGroupValueType[j] == 2)
                    {
                      primitiveLongArrayWrapperTmp.data = (long[])rawGroupValue;
                      rawGroupValue = primitiveLongArrayWrapperTmp;
                    }
                    else if (rawGroupValueType[j] == 0)
                    {
                      if (rawGroupValue != null)
                      {
                        if (rawGroupValue instanceof long[])
                          rawGroupValueType[j] = 2;
                        else
                          rawGroupValueType[j] = 1;
                      }
                    }

                    if (firstRawGroupValue == null) firstRawGroupValue = rawGroupValue;

                    pre = valueDocMaps[j].get(rawGroupValue);
                    if (pre != null)
                    {
                      j = -1;
                      break;
                    }

                    BrowseFacet facet = combinedFacetAccessibles[j].getFacet(sortCollector.groupByMulti[j].getName());
                    if (facet == null || facet.getFacetValueHitCount() != 1)
                      break;
                  }
                  if (j < 0)
                  {
                    if (tmpScoreDoc.sortValue.compareTo(pre.sortValue) < 0)
                    {
                      tmpScoreDoc.groupPos = pre.groupPos;
                      MyScoreDoc tmp = pre;
                      bottom = (MyScoreDoc)docQueue.replace(tmpScoreDoc, pre);
                      valueDocMaps[j].put(rawGroupValue, tmpScoreDoc);
                      tmpScoreDoc = tmp;
                    }
                  }
                  else
                  {
                    if (j >= sortCollector.groupByMulti.length)
                    {
                       j = 0;
                       rawGroupValue = firstRawGroupValue;
                    }
                    if (queueFull){
                      tmpScoreDoc.groupPos = j;
                      MyScoreDoc tmp = bottom;

                      vals = sortCollector.groupByMulti[j].getRawFieldValues(tmp.reader, tmp.doc);
                      if (vals != null && vals.length > 0) valueDocMaps[j].remove(vals[0]);

                      bottom = (MyScoreDoc)docQueue.replace(tmpScoreDoc);
                      valueDocMaps[j].put(rawGroupValue, tmpScoreDoc);
                      tmpScoreDoc = tmp;
                    }
                    else{ 
                      MyScoreDoc tmp = new MyScoreDoc(tmpScoreDoc.doc, tmpScoreDoc.score, currentContext.base + totalDocs + tmpScoreDoc.doc, currentContext.reader);
                      tmp.groupPos = j;
                      bottom = (MyScoreDoc)docQueue.add(tmp);
                      valueDocMaps[j].put(rawGroupValue, tmp);
                      queueFull = (docQueue.size >= topHits);
                    }
                  }
                }
              }
            }
            totalDocs += res.getTotalDocs();
          }
          while(null != (tmpScoreDoc = (MyScoreDoc)docQueue.pop()))
          {
            hitsList.add(tmpScoreDoc.getSenseiHit(req.isFetchStoredFields(),
                                                  req.isFetchStoredValue()));
          }
        }
        else
        {
          while(mergedIter.hasNext())
          {
            SenseiHit hit = mergedIter.next();
            firstRawGroupValue = null;
            int i=0;
            for (; i<groupSets.length; ++i)
            {
              //rawGroupValue = hit.getRawField(req.getGroupBy()[i]);
              rawGroupValue = hit.getRawGroupValue();
              if (rawGroupValueType[i] == 0)
              {
                if (rawGroupValue != null)
                {
                  if (rawGroupValue instanceof long[])
                    rawGroupValueType[i] = 2;
                  else
                    rawGroupValueType[i] = 1;
                }
              }
              if (rawGroupValueType[i] == 2)
              {
                primitiveLongArrayWrapperTmp.data = (long[])rawGroupValue;
                rawGroupValue = primitiveLongArrayWrapperTmp;
              }
              if (firstRawGroupValue == null) firstRawGroupValue = rawGroupValue;
              if (groupSets[i].contains(rawGroupValue))
              {
                i = -1;
                break;
              }

              //BrowseFacet facet = combinedFacetAccessibles[i].getFacet(hit.getField(req.getGroupBy()[i]));
              BrowseFacet facet = combinedFacetAccessibles[i].getFacet(hit.getGroupValue());
              if (facet == null || facet.getFacetValueHitCount() != 1)
                break;
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
                //hit.setGroupValue(hit.getField(req.getGroupBy()[i]));
                //hit.setRawGroupValue(hit.getRawField(req.getGroupBy()[i]));
                BrowseFacet facet = combinedFacetAccessibles[i].getFacet(hit.getGroupValue());
                if (facet != null)
                  hit.setGroupHitsCount(facet.getFacetValueHitCount());
                hitsList.add(hit);
                if (hitsList.size() >= req.getCount())
                  break;
              }
              if (rawGroupValueType[i] == 2)
                groupSets[i].add(new PrimitiveLongArrayWrapper(primitiveLongArrayWrapperTmp.data));
              else
                groupSets[i].add(rawGroupValue);
            }
          }
        }
        for (int i=0; i<combinedFacetAccessibles.length; ++i) combinedFacetAccessibles[i].close();
      }
      hits = hitsList.toArray(new SenseiHit[hitsList.size()]);

      if (req.getMaxPerGroup() > 1)
      {
        Map<Object, HitWithGroupQueue> groupMap = new HashMap<Object, HitWithGroupQueue>(hits.length*2);
        for (SenseiHit hit : hits)
        {
          rawGroupValue = hit.getRawField(req.getGroupBy()[hit.getGroupPosition()]);
          if (rawGroupValueType[hit.getGroupPosition()] == 0) {
            if (rawGroupValue != null)
            {
              if (rawGroupValue instanceof long[])
                rawGroupValueType[hit.getGroupPosition()] = 2;
              else
                rawGroupValueType[hit.getGroupPosition()] = 1;
            }
          }
          if (rawGroupValueType[hit.getGroupPosition()] == 2)
            rawGroupValue = new PrimitiveLongArrayWrapper((long[])rawGroupValue);

          if (req.getGroupBy().length > 1)
          {
            DeepObjectArrayWrapper wrapper = new DeepObjectArrayWrapper(new Object[2]);
            wrapper.data[0] = hit.getGroupPosition();
            wrapper.data[1] = rawGroupValue;
            rawGroupValue = wrapper;
          }

          groupMap.put(rawGroupValue, new HitWithGroupQueue(hit, new PriorityQueue<MyScoreDoc>()
            {
              private int r;

              {
                this.initialize(req.getMaxPerGroup());
              }

              protected boolean lessThan(MyScoreDoc a, MyScoreDoc b)
              {
                r = a.sortValue.compareTo(b.sortValue);
                if (r>0)
                  return true;
                else if (r<0)
                  return false;
                else
                  return (a.finalDoc > b.finalDoc);
              }
            }
          ));
        }

        MyScoreDoc tmpScoreDoc = null;
        int doc = 0;
        float score = 0.0f;
        Object[] vals = null;
        HitWithGroupQueue hitWithGroupQueue = null;
        DeepObjectArrayWrapper tmpWrapper = new DeepObjectArrayWrapper(new Object[2]);

        totalDocs = 0;
        for (SenseiResult res : results)
        {
          if (hasSortCollector)
          {
            SortCollector sortCollector = res.getSortCollector();
            Iterator<CollectorContext> contextIter = sortCollector.contextList.iterator();
            CollectorContext currentContext = null;
            int contextLeft = 0;
            while (contextIter.hasNext()) {
              currentContext = contextIter.next();
              contextLeft = currentContext.length;
              if (contextLeft > 0)
                break;
            }

            Iterator<float[]> scoreArrayIter = sortCollector.scorearraylist != null ? sortCollector.scorearraylist.iterator():null;
            if (contextLeft > 0)
            {
              for (int[] docs : sortCollector.docidarraylist)
              {
                float[] scores = scoreArrayIter != null ? scoreArrayIter.next():null;
                for (int i=0; i<SortCollector.BLOCK_SIZE; ++i)
                {
                  doc = docs[i];
                  score = scores != null ? scores[i]:0.0f;
                  int j=0;
                  for (; j<sortCollector.groupByMulti.length; ++j)
                  {
                    vals = sortCollector.groupByMulti[j].getRawFieldValues(currentContext.reader, doc);
                    if (vals != null && vals.length > 0)
                      rawGroupValue = vals[0];
                    else
                      rawGroupValue = null;
                    if (rawGroupValueType[j] == 2)
                    {
                      primitiveLongArrayWrapperTmp.data = (long[])rawGroupValue;
                      rawGroupValue = primitiveLongArrayWrapperTmp;
                    }
                    else if (rawGroupValueType[j] == 0)
                    {
                      if (rawGroupValue != null)
                      {
                        if (rawGroupValue instanceof long[])
                          rawGroupValueType[j] = 2;
                        else
                          rawGroupValueType[j] = 1;
                      }
                    }

                    if (sortCollector.groupByMulti.length > 1)
                    {
                      tmpWrapper.data[0] = j;
                      tmpWrapper.data[1] = rawGroupValue;
                      rawGroupValue = tmpWrapper;
                    }

                    hitWithGroupQueue = groupMap.get(rawGroupValue);
                    if (hitWithGroupQueue != null)
                    {
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
                      tmpScoreDoc = hitWithGroupQueue.queue.insertWithOverflow(tmpScoreDoc);
                    }
                  }
                  --contextLeft;
                  if (contextLeft <= 0)
                  {
                    while (contextIter.hasNext()) {
                      currentContext = contextIter.next();
                      contextLeft = currentContext.length;
                      if (contextLeft > 0)
                        break;
                    }
                    if (contextLeft <= 0) // No more docs left.
                      break;
                  }
                }
              }
            }
            sortCollector.close();
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
                  if (rawGroupValueType[hit.getGroupPosition()] == 2)
                  {
                    primitiveLongArrayWrapperTmp.data = (long[])rawGroupValue;
                    rawGroupValue = primitiveLongArrayWrapperTmp;
                  }

                  hitWithGroupQueue = groupMap.get(rawGroupValue);
                  if (hitWithGroupQueue != null)
                    hitWithGroupQueue.iterList.add(Arrays.asList(hit.getSenseiGroupHits()).iterator());
                }
              }
            }
          }
          totalDocs += res.getTotalDocs();
        }

        if (hasSortCollector)
        {
          for (HitWithGroupQueue hwg : groupMap.values())
          {
            int index = hwg.queue.size() - 1;
            if (index >= 0)
            {
              SenseiHit[] groupHits = new SenseiHit[index+1];
              while (index >=0)
              {
                groupHits[index] = hwg.queue.pop().getSenseiHit(req.isFetchStoredFields(),
                                                                req.isFetchStoredValue());
                --index;
              }
              hwg.hit.setGroupHits(groupHits);
            }
          }
        }
        else
        {
          for (HitWithGroupQueue hwg : groupMap.values())
          {
            List<SenseiHit> mergedList = ListMerger.mergeLists(0, req.getMaxPerGroup(), hwg.iterList
                .toArray(new Iterator[hwg.iterList.size()]), comparator);
            SenseiHit[] groupHits = mergedList.toArray(new SenseiHit[mergedList.size()]);
            hwg.hit.setGroupHits(groupHits);
          }
        }
      }
    }

    SenseiResult merged = new SenseiResult();
    merged.setHits(hits);
    merged.setNumHits(numHits);
    merged.setNumGroups(numGroups);
    //merged.setGroupMap(groupMap);
    merged.setTotalDocs(totalDocs);
    merged.addAll(mergedFacetMap);
    
    if (parsedQuery == null){
    	parsedQuery = "";
    }
    
    long end = System.currentTimeMillis();
    
    time += (end-start);
    merged.setTime(time);
    merged.setParsedQuery(parsedQuery);
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
}
