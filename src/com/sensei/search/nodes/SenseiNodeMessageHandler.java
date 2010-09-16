package com.sensei.search.nodes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import proj.zoie.api.IndexReaderFactory;
import proj.zoie.api.ZoieIndexReader;
import proj.zoie.api.ZoieIndexReader.SubReaderAccessor;
import proj.zoie.api.ZoieIndexReader.SubReaderInfo;

import com.browseengine.bobo.api.BoboBrowser;
import com.browseengine.bobo.api.BoboIndexReader;
import com.browseengine.bobo.api.BrowseException;
import com.browseengine.bobo.api.BrowseHit;
import com.browseengine.bobo.api.BrowseRequest;
import com.browseengine.bobo.api.FacetAccessible;
import com.browseengine.bobo.api.MultiBoboBrowser;
import com.browseengine.bobo.sort.SortCollector;
import com.google.protobuf.Message;
import com.google.protobuf.TextFormat;
import com.linkedin.norbert.javacompat.network.MessageHandler;
import com.sensei.search.client.ResultMerger;
import com.sensei.search.req.SenseiHit;
import com.sensei.search.req.SenseiRequest;
import com.sensei.search.req.SenseiResult;
import com.sensei.search.req.protobuf.SenseiRequestBPO;
import com.sensei.search.req.protobuf.SenseiRequestBPOConverter;
import com.sensei.search.req.protobuf.SenseiResultBPO;
import com.sensei.search.util.RequestConverter;

public class SenseiNodeMessageHandler implements MessageHandler {

	private static final Logger logger = Logger.getLogger(SenseiNodeMessageHandler.class);
	private final Map<Integer,SenseiQueryBuilderFactory> _builderFactoryMap;
	private final Map<Integer,IndexReaderFactory<ZoieIndexReader<BoboIndexReader>>> _partReaderMap;

	public SenseiNodeMessageHandler(SenseiSearchContext ctx) {
		_builderFactoryMap = ctx.getQueryBuilderFactoryMap();
		_partReaderMap = ctx.getPartitionReaderMap();
	}
	
	public int[] getPartitions(){
		Set<Integer> partSet = _partReaderMap.keySet();
		int[] retSet = new int[partSet.size()];
		int c = 0;
		for (Integer part : partSet){
			retSet[c++] = part;
		}
		return retSet;
	}

	public Message[] getMessages() {
		return new Message[] { SenseiRequestBPO.Request.getDefaultInstance() };
	}
	
	private SenseiResult handleMessage(SenseiRequest senseiReq,IndexReaderFactory<ZoieIndexReader<BoboIndexReader>> readerFactory, int partition) throws Exception{
		List<ZoieIndexReader<BoboIndexReader>> readerList = null;
		
		try{
		  readerList = readerFactory.getIndexReaders();
		  
		  if (readerList == null || readerList.size() == 0){
			logger.warn("no readers were obtained from zoie, returning no hits.");
			return new SenseiResult();
		  }
		  
		  List<BoboIndexReader> boboReaders = ZoieIndexReader.extractDecoratedReaders(readerList);
	      SubReaderAccessor<BoboIndexReader> subReaderAccessor = ZoieIndexReader.getSubReaderAccessor(boboReaders);

		  MultiBoboBrowser browser = null;

		  try {
		    browser = new MultiBoboBrowser(BoboBrowser.createBrowsables(boboReaders));
		    
		    BrowseRequest breq = RequestConverter.convert(senseiReq, _builderFactoryMap.get(partition));
		    SenseiResult res = browse(browser, breq, subReaderAccessor);
		    return res;
		  } 
		  catch(Exception e){
		    logger.error(e.getMessage(),e);
		    throw e;
		  }
		  finally {
		    if (browser != null) {
		      try {
		        browser.close();
		      } catch (IOException ioe) {
		        logger.error(ioe.getMessage(), ioe);
		      }
		    }
		  }
		}
		finally{
			if (readerList!=null){
				readerFactory.returnIndexReaders(readerList);
			}
		}
	}
	
	private SenseiResult browse(MultiBoboBrowser browser, BrowseRequest req, SubReaderAccessor<BoboIndexReader> subReaderAccessor) throws BrowseException
	{
	  final SenseiResult result = new SenseiResult();

	  long start = System.currentTimeMillis();
	  int offset = req.getOffset();
	  int count = req.getCount();
	  
	  if (offset<0 || count<0){
	    throw new IllegalArgumentException("both offset and count must be > 0: "+offset+"/"+count);
	  }
	  SortCollector collector = browser.getSortCollector(req.getSort(),req.getQuery(), offset, count, req.isFetchStoredFields(),false);
	  
	  Map<String, FacetAccessible> facetCollectors = new HashMap<String, FacetAccessible>();
	  browser.browse(req, collector, facetCollectors);
	  BrowseHit[] hits = null;
	  try{
	    hits = collector.topDocs();
	  }
	  catch (IOException e){
	    logger.error(e.getMessage(), e);
	    hits = new BrowseHit[0];
	  }
	  SenseiHit[] senseiHits = new SenseiHit[hits.length];
	  for(int i = 0; i < hits.length; i++)
	  {
	    BrowseHit hit = hits[i];
	    SenseiHit senseiHit = new SenseiHit();
	    
        int docid = hit.getDocid();
        SubReaderInfo<BoboIndexReader> readerInfo = subReaderAccessor.getSubReaderInfo(docid);
        int uid = (int)((ZoieIndexReader<BoboIndexReader>)readerInfo.subreader.getInnerReader()).getUID(readerInfo.subdocid);
        senseiHit.setUID(uid);
        senseiHit.setDocid(docid);
        senseiHit.setScore(hit.getScore());
        senseiHit.setComparable(hit.getComparable());
        senseiHit.setFieldValues(hit.getFieldValues());
        senseiHit.setStoredFields(hit.getStoredFields());
        senseiHit.setExplanation(hit.getExplanation());
        
	    senseiHits[i] = senseiHit;
	  }
	  result.setHits(senseiHits);
	  result.setNumHits(collector.getTotalHits());
	  result.setTotalDocs(browser.numDocs());
	  result.addAll(facetCollectors);
	  long end = System.currentTimeMillis();
	  result.setTime(end - start);
	  // set the transaction ID to trace transactions
	  result.setTid(req.getTid());
	  return result;
	}
	  
	public Message handleMessage(Message msg) throws Exception {
		SenseiRequestBPO.Request req = (SenseiRequestBPO.Request) msg;
		
		if (logger.isDebugEnabled()){
		  String reqString = TextFormat.printToString(req);
		  reqString = reqString.replace('\r', ' ').replace('\n', ' ');
		}

		SenseiRequest senseiReq = SenseiRequestBPOConverter.convert(req);
		
		SenseiResult finalResult=null;
		Set<Integer> partitions = senseiReq.getPartitions();
		if (partitions!=null && partitions.size() > 0){
			logger.info("serving partitions: "+ partitions.toString());
			ArrayList<SenseiResult> resultList = new ArrayList<SenseiResult>(partitions.size());
			for (int partition : partitions){
			  try{
				long start = System.currentTimeMillis();
			    IndexReaderFactory<ZoieIndexReader<BoboIndexReader>> readerFactory=_partReaderMap.get(partition);
			    SenseiResult res = handleMessage(senseiReq, readerFactory, partition);
			    resultList.add(res);
			    long end = System.currentTimeMillis();
			    logger.info("searching partition: "+partition+" took: "+(end-start));
			  }
			  catch(Exception e){
				  logger.error(e.getMessage(),e);
			  }
			}

            finalResult = ResultMerger.merge(senseiReq, resultList,true);
		}
		else{
			logger.info("no partitions specified");
			finalResult = new SenseiResult();
		}
		return SenseiRequestBPOConverter.convert(finalResult);
	}

}
