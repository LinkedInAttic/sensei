package com.sensei.search.svc.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.management.ObjectName;

import org.apache.log4j.Logger;
import org.apache.lucene.search.Query;

import proj.zoie.api.ZoieIndexReader;
import proj.zoie.api.ZoieIndexReader.SubReaderAccessor;
import proj.zoie.api.ZoieIndexReader.SubReaderInfo;

import com.browseengine.bobo.api.BoboBrowser;
import com.browseengine.bobo.api.BoboIndexReader;
import com.browseengine.bobo.api.BrowseException;
import com.browseengine.bobo.api.BrowseHit;
import com.browseengine.bobo.api.BrowseRequest;
import com.browseengine.bobo.api.BrowseResult;
import com.browseengine.bobo.api.FacetAccessible;
import com.browseengine.bobo.api.MultiBoboBrowser;
import com.google.protobuf.Message;
import com.sensei.indexing.api.SenseiIndexPruner;
import com.sensei.indexing.api.SenseiIndexPruner.IndexReaderSelector;
import com.sensei.search.client.ResultMerger;
import com.sensei.search.jmx.JmxUtil;
import com.sensei.search.jmx.JmxUtil.Timer;
import com.sensei.search.nodes.SenseiCore;
import com.sensei.search.nodes.SenseiQueryBuilderFactory;
import com.sensei.search.req.SenseiHit;
import com.sensei.search.req.SenseiRequest;
import com.sensei.search.req.SenseiResult;
import com.sensei.search.req.protobuf.SenseiRequestBPO;
import com.sensei.search.req.protobuf.SenseiRequestBPOConverter;
import com.sensei.search.util.RequestConverter;
import com.yammer.metrics.core.TimerMetric;

public class CoreSenseiServiceImpl extends AbstractSenseiCoreService<SenseiRequest, SenseiResult>{

	private static final Logger logger = Logger.getLogger(CoreSenseiServiceImpl.class);
	

	private final static TimerMetric PruneTimer = new TimerMetric(TimeUnit.MILLISECONDS,TimeUnit.SECONDS);
	static{
		  // register jmx monitoring for timers
		  try{
		    ObjectName pruneMBeanName = new ObjectName(JmxUtil.Domain+".node","name","prune-time");
		    JmxUtil.registerMBean(PruneTimer, pruneMBeanName);
		  }
		  catch(Exception e){
				logger.error(e.getMessage(),e);
		  }
	}
	public CoreSenseiServiceImpl(SenseiCore core) {
		super(core);
	}
	
	private SenseiResult browse(MultiBoboBrowser browser, BrowseRequest req, SubReaderAccessor<BoboIndexReader> subReaderAccessor) throws BrowseException
	  {
	    final SenseiResult result = new SenseiResult();

	    long start = System.currentTimeMillis();
	    int offset = req.getOffset();
	    int count = req.getCount();

	    if (offset < 0 || count < 0)
	    {
	      throw new IllegalArgumentException("both offset and count must be > 0: " + offset + "/" + count);
	    }
	    // SortCollector collector =
	    // browser.getSortCollector(req.getSort(),req.getQuery(), offset, count,
	    // req.isFetchStoredFields(),false);

	    // Map<String, FacetAccessible> facetCollectors = new HashMap<String,
	    // FacetAccessible>();
	    // browser.browse(req, collector, facetCollectors);
	    BrowseResult res = browser.browse(req);
	    BrowseHit[] hits = res.getHits();

	    SenseiHit[] senseiHits = new SenseiHit[hits.length];
	    for (int i = 0; i < hits.length; i++)
	    {
	      BrowseHit hit = hits[i];
	      SenseiHit senseiHit = new SenseiHit();

	      int docid = hit.getDocid();
	      SubReaderInfo<BoboIndexReader> readerInfo = subReaderAccessor.getSubReaderInfo(docid);
	      long uid = (long) ((ZoieIndexReader<BoboIndexReader>) readerInfo.subreader.getInnerReader()).getUID(readerInfo.subdocid);
	      senseiHit.setUID(uid);
	      senseiHit.setDocid(docid);
	      senseiHit.setScore(hit.getScore());
	      senseiHit.setComparable(hit.getComparable());
	      senseiHit.setFieldValues(hit.getFieldValues());
	      senseiHit.setStoredFields(hit.getStoredFields());
	      senseiHit.setExplanation(hit.getExplanation());
	      senseiHit.setGroupValue(hit.getGroupValue());
	      senseiHit.setRawGroupValue(hit.getRawGroupValue());
	      senseiHit.setGroupHitsCount(hit.getGroupHitsCount());

	      senseiHits[i] = senseiHit;
	    }
	    result.setHits(senseiHits);
	    result.setNumHits(res.getNumHits());
	    result.setNumGroups(res.getNumGroups());
	    result.setGroupAccessible(res.getGroupAccessible());
	    result.setSortCollector(res.getSortCollector());
	    result.setTotalDocs(browser.numDocs());
	    

	    Map<String, FacetAccessible> facetMap = res.getFacetMap();
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
		SubReaderAccessor<BoboIndexReader> subReaderAccessor = ZoieIndexReader.getSubReaderAccessor(readerList);
	    MultiBoboBrowser browser = null;
	    
	    
	    
	    try
	    {
          final List<BoboIndexReader> segmentReaders = BoboBrowser.gatherSubReaders(readerList);
          if (segmentReaders!=null && segmentReaders.size()>0){
        	final AtomicInteger skipDocs = new AtomicInteger(0);
        	List<BoboIndexReader> validatedSegmentReaders = PruneTimer.time(new Callable<List<BoboIndexReader>>(){

				@Override
				public List<BoboIndexReader> call() throws Exception {
					SenseiIndexPruner pruner = _core.getIndexPruner();

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
        	
        	
	        browser = new MultiBoboBrowser(BoboBrowser.createBrowsables(validatedSegmentReaders));
	        BrowseRequest breq = RequestConverter.convert(request, queryBuilderFactory);
          breq.setCollectDocIdCache(true);
	        SenseiResult res = browse(browser, breq, subReaderAccessor);
	        int totalDocs = res.getTotalDocs()+skipDocs.get();
	        res.setTotalDocs(totalDocs);
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
		return ResultMerger.merge(r, resultList, true);
	}

	@Override
	public SenseiResult getEmptyResultInstance(Throwable error) {
		return new SenseiResult();
	} 
	  
	@Override
	public Message resultToMessage(SenseiResult result) {
		return SenseiRequestBPOConverter.convert(result);
	}

	@Override
	public SenseiRequest reqFromMessage(Message req) {
		return SenseiRequestBPOConverter.convert((SenseiRequestBPO.Request)req);
	}

	@Override
	public Message getEmptyRequestInstance() {
		return SenseiRequestBPO.Request.getDefaultInstance(); 
	}
}
