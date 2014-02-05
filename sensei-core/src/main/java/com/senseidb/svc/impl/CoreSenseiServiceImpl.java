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
package com.senseidb.svc.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import com.browseengine.bobo.api.FacetAccessible;
import com.browseengine.bobo.facets.FacetHandler;
import com.sensei.search.req.protobuf.SenseiReqProtoSerializer;
import com.senseidb.search.req.*;
import org.apache.log4j.Logger;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Query;

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Weight;
import proj.zoie.api.ZoieIndexReader;
import proj.zoie.api.ZoieIndexReader.SubReaderAccessor;
import proj.zoie.api.ZoieIndexReader.SubReaderInfo;

import com.browseengine.bobo.api.BoboBrowser;
import com.browseengine.bobo.api.BoboIndexReader;
import com.browseengine.bobo.api.BrowseException;
import com.browseengine.bobo.api.BrowseHit;
import com.browseengine.bobo.api.BrowseRequest;
import com.browseengine.bobo.api.BrowseResult;
import com.browseengine.bobo.api.MultiBoboBrowser;
import com.browseengine.bobo.sort.SortCollector;
import com.linkedin.norbert.network.JavaSerializer;
import com.linkedin.norbert.network.Serializer;
import com.senseidb.indexing.SenseiIndexPruner;
import com.senseidb.indexing.SenseiIndexPruner.IndexReaderSelector;
import com.senseidb.search.node.ResultMerger;
import com.senseidb.search.node.SenseiCore;
import com.senseidb.search.node.SenseiQueryBuilderFactory;
import com.senseidb.search.req.SenseiHit;
import com.senseidb.search.req.SenseiRequest;
import com.senseidb.search.req.SenseiResult;
import com.senseidb.search.req.mapred.impl.SenseiMapFunctionWrapper;
import com.senseidb.util.RequestConverter;
import com.yammer.metrics.core.Timer;

import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_RESULT_HIT_UID;

public class CoreSenseiServiceImpl extends AbstractSenseiCoreService<SenseiRequest, SenseiResult> {
	public static final Serializer<SenseiRequest, SenseiResult> JAVA_SERIALIZER =
			JavaSerializer.apply("SenseiRequest", SenseiRequest.class, SenseiResult.class);

	public static final Serializer<SenseiRequest, SenseiResult> PROTO_SERIALIZER =
			new SenseiReqProtoSerializer();

  public static final Serializer<SenseiRequest, SenseiResult> PROTO_V2_SERIALIZER =
      new SenseiSnappyProtoSerializer();

	private static final Logger logger = Logger.getLogger(CoreSenseiServiceImpl.class);
  private static final String TOP_DOCS_METHOD = "topDocs";
  private static final String GET_HITS_FEATURES_METHOD = "getHitsFeatures";

  private final Timer _timerMetric;

	public CoreSenseiServiceImpl(SenseiCore core) {
		super(core);
    _timerMetric = registerTimer("prune");
	}

  @Override
  protected String getMetricScope()
  {
    return "node";
  }
	
	private SenseiResult browse(SenseiRequest senseiRequest, MultiBoboBrowser browser, BrowseRequest req, SubReaderAccessor<BoboIndexReader> subReaderAccessor) throws BrowseException, IOException, Exception {
	    final SenseiResult result = new SenseiResult();

	    long start = System.currentTimeMillis();
	    int offset = req.getOffset();
	    int count = req.getCount();

	    if (offset < 0 || count < 0)
	    {
	      throw new IllegalArgumentException("both offset and count must be > 0: " + offset + "/" + count);
	    }

      final Collector collector = senseiRequest.buildCollector(req.getQuery());
      BrowseResult res = null;
      BrowseHit[] hits = null;

      ScoreDoc[] scoreDocs = null;
      float [][] features = null;

      if (collector == null) {
        res = browser.browse(req);
        hits = res.getHits();
      }
      else {
        Map<String, FacetAccessible> facetCollectors = new HashMap<String, FacetAccessible>();
        Weight w = req.getQuery().createWeight(browser);
        browser.browse(req, w, collector, facetCollectors, offset);

        try {
          /**
           * A custom collector must implement topDocs() method and a getHitsFeatures() method.
           */
          scoreDocs = (ScoreDoc[]) collector.getClass().getMethod(TOP_DOCS_METHOD).invoke(collector);
          features = (float[][]) collector.getClass().getMethod(GET_HITS_FEATURES_METHOD).invoke(collector);
          hits = new BrowseHit[scoreDocs.length];
          Map<String, FacetHandler<?>> facetHandlerMap = browser.getFacetHandlerMap();

          int i = 0;

          for (ScoreDoc doc : scoreDocs) {
            BrowseHit hit = new BrowseHit();
            hit.setScore(doc.score);
            hit.setDocid(doc.doc);

            Map<String,String[]> map = new HashMap<String,String[]>();
            Map<String,Object[]> rawMap = new HashMap<String,Object[]>();

            for (Map.Entry<String, FacetHandler<?>> entry : facetHandlerMap.entrySet()) {
              map.put(entry.getKey(), browser.getFieldVal(doc.doc, entry.getKey()));
              rawMap.put(entry.getKey(), browser.getRawFieldVal(doc.doc, entry.getKey()));
            }

            hit.setFieldValues(map);
            hit.setRawFieldValues(rawMap);
            hits[i++] = hit;
          }

          res = new BrowseResult();
          res.setHits(hits);
          res.setNumHits(hits.length);

        } catch (Exception e) {
          logger.error(e.getMessage(), e);
        }

      }

	    if (req.getMapReduceWrapper() != null) {
	      result.setMapReduceResult(req.getMapReduceWrapper().getResult());
	    }

	    SenseiHit[] senseiHits = new SenseiHit[hits.length];
      Set<String> selectSet = senseiRequest.getSelectSet();
	    for (int i = 0; i < hits.length; i++)
	    {
	      BrowseHit hit = hits[i];
	      SenseiHit senseiHit = new SenseiHit();

	      int docid = hit.getDocid();
	      SubReaderInfo<BoboIndexReader> readerInfo = subReaderAccessor.getSubReaderInfo(docid);
        Long uid = (Long)hit.getRawField(PARAM_RESULT_HIT_UID);
        if (uid == null)
          uid = ((ZoieIndexReader<BoboIndexReader>) readerInfo.subreader.getInnerReader()).getUID(readerInfo.subdocid);
	      senseiHit.setUID(uid);
	      senseiHit.setDocid(docid);
	      senseiHit.setScore(hit.getScore());
	      senseiHit.setComparable(hit.getComparable());
        if (selectSet != null && selectSet.size() != 0)
        {
          // Clear the data those are not used:
          if (hit.getFieldValues() != null)
          {
            Iterator<String> iter = hit.getFieldValues().keySet().iterator();
            while (iter.hasNext())
            {
              if (!selectSet.contains(iter.next()))
              {
                iter.remove();
              }
            }
          }
          if (hit.getRawFieldValues() != null)
          {
            Iterator<String> iter = hit.getRawFieldValues().keySet().iterator();
            while (iter.hasNext())
            {
              if (!selectSet.contains(iter.next()))
              {
                iter.remove();
              }
            }
          }
        }

        if (features != null) {
          senseiHit.setFeatures(features[i]);
        }

        senseiHit.setFieldValues(hit.getFieldValues());
        senseiHit.setRawFieldValues(hit.getRawFieldValues());
	      senseiHit.setStoredFields(hit.getStoredFields());
	      senseiHit.setExplanation(hit.getExplanation());
	      senseiHit.setGroupField(hit.getGroupField());
	      senseiHit.setGroupValue(hit.getGroupValue());
	      senseiHit.setRawGroupValue(hit.getRawGroupValue());
	      senseiHit.setGroupHitsCount(hit.getGroupHitsCount());
	      senseiHit.setTermFreqMap(hit.getTermFreqMap());

	      senseiHits[i] = senseiHit;
	    }
	    result.setHits(senseiHits);
	    result.setNumHits(res.getNumHits());
	    result.setNumGroups(res.getNumGroups());
	    result.setGroupAccessibles(res.getGroupAccessibles());
	    result.setSortCollector(res.getSortCollector());
	    result.setTotalDocs(browser.numDocs());
	    
	    result.addAll(res.getFacetMap());
	    
      // Defer the closing of facetAccessibles till result merging time.
      
	    // Collection<FacetAccessible> facetAccessibles = facetMap.values();
	    // for (FacetAccessible facetAccessible : facetAccessibles){
	    // 	facetAccessible.close();
	    // }
	    
	    long end = System.currentTimeMillis();
	    result.setTime(end - start);
	    // set the transaction ID to trace transactions
	    result.setTid(req.getTid());

	    Query parsedQ = req.getQuery();
	    if (parsedQ != null)
	    {
	      result.setParsedQuery(parsedQ.toString());
	    } else
	    {
	      result.setParsedQuery("*:*");
	    }
	    return result;
	  }
	
	@Override
	public SenseiResult handlePartitionedRequest(final SenseiRequest request,
			List<BoboIndexReader> readerList,SenseiQueryBuilderFactory queryBuilderFactory) throws Exception {
	    MultiBoboBrowser browser = null;

	    try
	    {
          final List<BoboIndexReader> segmentReaders = BoboBrowser.gatherSubReaders(readerList);
          if (segmentReaders!=null && segmentReaders.size() > 0) {
            final AtomicInteger skipDocs = new AtomicInteger(0);

            final SenseiIndexPruner pruner = _core.getIndexPruner();

            List<BoboIndexReader> validatedSegmentReaders = _timerMetric.time(new Callable<List<BoboIndexReader>>(){

               @Override
               public List<BoboIndexReader> call() throws Exception {
                  IndexReaderSelector readerSelector = pruner.getReaderSelector(request);
                  List<BoboIndexReader> validatedReaders = new ArrayList<BoboIndexReader>(segmentReaders.size());
                  for (BoboIndexReader segmentReader : segmentReaders){
                    if (readerSelector.isSelected(segmentReader)){
                      validatedReaders.add(segmentReader);
                    }
                    else{
                      skipDocs.addAndGet(segmentReader.numDocs());
                    }
                  }
                  return validatedReaders;
                }

            });

            pruner.sort(validatedSegmentReaders);

            browser = new MultiBoboBrowser(BoboBrowser.createBrowsables(validatedSegmentReaders));
            request.setSearchable(browser);
            request.setQueryBuilderFactory(queryBuilderFactory);
            BrowseRequest breq = RequestConverter.convert(request, queryBuilderFactory);
            if (request.getMapReduceFunction() != null) {
              SenseiMapFunctionWrapper mapWrapper = new SenseiMapFunctionWrapper(request.getMapReduceFunction(), _core.getSystemInfo().getFacetInfos());
              breq.setMapReduceWrapper(mapWrapper);
            }
            SubReaderAccessor<BoboIndexReader> subReaderAccessor =
                ZoieIndexReader.getSubReaderAccessor(validatedSegmentReaders);
            SenseiResult res = browse(request, browser, breq, subReaderAccessor);
            int totalDocs = res.getTotalDocs()+skipDocs.get();
            res.setTotalDocs(totalDocs);

            // For debugging serialization issues:
//            byte[] responseBytes = getSerializer().responseToBytes(res);
//            SenseiResult response2 = getSerializer().responseFromBytes(responseBytes);
//            if(!res.equals(response2)) {
//              throw new IllegalArgumentException("Cant serialize response");
//            }

            return res;
          }
          else{
        	  return new SenseiResult();
          }
	    } catch (Exception e)
	    {
	      logger.error(e.getMessage(), e);
	      throw e;
	    } finally
	    {
	      if (browser != null)
	      {
	        try
	        {
	          browser.close();
	        } catch (IOException ioe)
	        {
	          logger.error(ioe.getMessage(), ioe);
	        }
	      }
	    }
	}

	@Override
	public SenseiResult mergePartitionedResults(SenseiRequest r,
			List<SenseiResult> resultList) {
    try
    {
      return ResultMerger.merge(r, resultList, true);
    }
    finally
    {
      if (resultList != null)
      {
        for (SenseiResult res : resultList)
        {
          SortCollector sortCollector = res.getSortCollector();
          if (sortCollector != null)
          {
            sortCollector.close();
          }
        }
      }
    }
	}

	@Override
	public SenseiResult getEmptyResultInstance(Throwable error) {
		return new SenseiResult();
	}

	@Override
	public Serializer<SenseiRequest, SenseiResult> getSerializer() {
		 return PROTO_V2_SERIALIZER;
	}
}
