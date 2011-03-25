package com.sensei.search.svc.impl;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

import proj.zoie.api.IndexReaderFactory;
import proj.zoie.api.ZoieIndexReader;

import com.browseengine.bobo.api.BoboIndexReader;
import com.google.protobuf.Message;
import com.sensei.search.nodes.SenseiCore;
import com.sensei.search.nodes.SenseiQueryBuilderFactory;
import com.sensei.search.req.AbstractSenseiRequest;
import com.sensei.search.req.AbstractSenseiResult;

public abstract class AbstractSenseiCoreService<Req extends AbstractSenseiRequest,Res extends AbstractSenseiResult>{
    private final static Logger logger = Logger.getLogger(AbstractSenseiCoreService.class);
    
	private final SenseiCore _core;
	
	public AbstractSenseiCoreService(SenseiCore core){
	  _core = core;
	}
	
	public final Res execute(Req senseiReq){
		Set<Integer> partitions = senseiReq==null ? null : senseiReq.getPartitions();
		if (partitions==null){
			partitions = new HashSet<Integer>();
			int[] containsPart = _core.getPartitions();
			if (containsPart!=null){
			  for (int part : containsPart){
			    partitions.add(part);
			  }
			}
		}
		Res finalResult;
	    if (partitions != null && partitions.size() > 0)
	    {
	      if (logger.isInfoEnabled()){
	        logger.info("serving partitions: " + partitions.toString());
	      }
	      ArrayList<Res> resultList = new ArrayList<Res>(partitions.size());
	      for (int partition : partitions)
	      {
	        try
	        {
	          long start = System.currentTimeMillis();
	          IndexReaderFactory<ZoieIndexReader<BoboIndexReader>> readerFactory = _core.getIndexReaderFactory(partition);
	          Res res = handleRequest(senseiReq, readerFactory,_core.getQueryBuilderFactory(partition));
	          resultList.add(res);
	          long end = System.currentTimeMillis();
	          res.setTime(end - start);
	          logger.info("searching partition: " + partition + " browse took: " + res.getTime());
	        } catch (Exception e)
	        {
	          logger.error(e.getMessage(), e);
	          resultList.add(getEmptyResultInstance(e));
	        }
	      }

	      finalResult = mergePartitionedResults(senseiReq, resultList);
	    } else
	    {
	      if (logger.isInfoEnabled()){
	        logger.info("no partitions specified");
	      }
	      finalResult = getEmptyResultInstance(null);
	    }
	    if (logger.isInfoEnabled()){
	      logger.info("searching partitions  " + String.valueOf(partitions) + " took: " + finalResult.getTime());
	    }
	    return finalResult;
	}
	
	private final Res handleRequest(Req senseiReq,IndexReaderFactory<ZoieIndexReader<BoboIndexReader>> readerFactory,SenseiQueryBuilderFactory queryBuilderFactory) throws Exception{
		List<ZoieIndexReader<BoboIndexReader>> readerList = null;
        try{
      	  readerList = readerFactory.getIndexReaders();
      	  if (logger.isDebugEnabled()){
      		  logger.debug("obtained readerList of size: "+readerList==null? 0 : readerList.size());
      	  }
      	  List<BoboIndexReader> boboReaders = ZoieIndexReader.extractDecoratedReaders(readerList);
      	  return handlePartitionedRequest(senseiReq,boboReaders,queryBuilderFactory);
        }
        finally{
          if (readerFactory!=readerList){
          	readerFactory.returnIndexReaders(readerList);
          }
        }
	}
	
	public abstract Res handlePartitionedRequest(Req r,final List<BoboIndexReader> readerList,SenseiQueryBuilderFactory queryBuilderFactory) throws Exception;
	public abstract Res mergePartitionedResults(Req r,List<Res> reqList);
	public abstract Res getEmptyResultInstance(Throwable error);
	public abstract Message getEmptyRequestInstance();
	
	public abstract Message resultToMessage(Res result);
	public abstract Req reqFromMessage(Message req);
}
