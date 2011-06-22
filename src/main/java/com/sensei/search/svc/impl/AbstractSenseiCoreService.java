package com.sensei.search.svc.impl;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.management.ObjectName;

import org.apache.log4j.Logger;

import proj.zoie.api.IndexReaderFactory;
import proj.zoie.api.ZoieIndexReader;

import com.browseengine.bobo.api.BoboIndexReader;
import com.google.protobuf.Message;
import com.sensei.search.jmx.JmxUtil;
import com.sensei.search.jmx.JmxUtil.Timer;
import com.sensei.search.nodes.SenseiCore;
import com.sensei.search.nodes.SenseiQueryBuilderFactory;
import com.sensei.search.req.AbstractSenseiRequest;
import com.sensei.search.req.AbstractSenseiResult;
import com.yammer.metrics.core.TimerMetric;
import com.yammer.metrics.reporting.JmxReporter.Counter;

public abstract class AbstractSenseiCoreService<Req extends AbstractSenseiRequest,Res extends AbstractSenseiResult>{
  private final static Logger logger = Logger.getLogger(AbstractSenseiCoreService.class);
  

  private final static TimerMetric GetReaderTimer = new TimerMetric(TimeUnit.MILLISECONDS,TimeUnit.SECONDS);
  private final static TimerMetric SearchTimer = new TimerMetric(TimeUnit.MILLISECONDS,TimeUnit.SECONDS);
  private final static TimerMetric MergeTimer = new TimerMetric(TimeUnit.MILLISECONDS,TimeUnit.SECONDS);
	
  static{
	  // register jmx monitoring for timers
	  try{
	    ObjectName getReaderMBeanName = new ObjectName(JmxUtil.Domain+".node","name","getreader-time");
	    JmxUtil.registerMBean(GetReaderTimer, getReaderMBeanName);
	    
	    ObjectName searchMBeanName = new ObjectName(JmxUtil.Domain+".node","name","search-time");
	    JmxUtil.registerMBean(SearchTimer, searchMBeanName);

	    ObjectName mergeMBeanName = new ObjectName(JmxUtil.Domain+".node","name","merge-time");
	    JmxUtil.registerMBean(MergeTimer, mergeMBeanName); 
	  }
	  catch(Exception e){
		logger.error(e.getMessage(),e);
	  }
  }
  protected long _timeout = 8000;
    
  protected final SenseiCore _core;

  private final ExecutorService _executorService = Executors.newCachedThreadPool();
	
	public AbstractSenseiCoreService(SenseiCore core){
	  _core = core;
	}
	
	public final Res execute(final Req senseiReq){
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
	      if (logger.isDebugEnabled()){
	        logger.debug("serving partitions: " + partitions.toString());
	      }
	      final ArrayList<Res> resultList = new ArrayList<Res>(partitions.size());
        Future<Res>[] futures = new Future[partitions.size()-1];
        int i = 0;
	      for (final int partition : partitions)
	      {
          final long start = System.currentTimeMillis();
          final IndexReaderFactory<ZoieIndexReader<BoboIndexReader>> readerFactory = _core.getIndexReaderFactory(partition);

          if (i<partitions.size()-1)  // Search simultaneously.
          {
            try
            {
              futures[i] = (Future<Res>)_executorService.submit(new Callable<Res>()
              {
                public Res call() throws Exception
                {
                  Res res = handleRequest(senseiReq, readerFactory, _core.getQueryBuilderFactory());

                  long end = System.currentTimeMillis();
                  res.setTime(end - start);
                  logger.info("searching partition: " + partition + " browse took: " + res.getTime());

                  return res;
                }
              });
            } catch (Exception e)
            {
              logger.error(e.getMessage(), e);
            }
          }
          else  // Reuse current thread.
          {
            try
            {
              Res res = handleRequest(senseiReq, readerFactory, _core.getQueryBuilderFactory());
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
          ++i;
	      }

        for (i=0; i<futures.length; ++i)
        {
          try
          {
            Res res = futures[i].get(_timeout, TimeUnit.MILLISECONDS);
            resultList.add(res);
          }
          catch(Exception e)
          {
	          logger.error(e.getMessage(), e);
	          resultList.add(getEmptyResultInstance(e));
          }
        }

          try{
	        finalResult = MergeTimer.time(new Callable<Res>(){
	    	 public Res call() throws Exception{
	    	   return mergePartitionedResults(senseiReq, resultList);
	    	 }
	        });
          }
          catch(Exception e){
        	logger.error(e.getMessage(),e);
        	finalResult = getEmptyResultInstance(null);
          }
	    } 
	    else
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
	
	private final Res handleRequest(final Req senseiReq,final IndexReaderFactory<ZoieIndexReader<BoboIndexReader>> readerFactory,final SenseiQueryBuilderFactory queryBuilderFactory) throws Exception{
		List<ZoieIndexReader<BoboIndexReader>> readerList = null;
        try{
      	  readerList = GetReaderTimer.time(new Callable<List<ZoieIndexReader<BoboIndexReader>>>(){
      		 public List<ZoieIndexReader<BoboIndexReader>> call() throws Exception{
      			return readerFactory.getIndexReaders(); 
      		 }
      	  });
      	  if (logger.isDebugEnabled()){
      		  logger.debug("obtained readerList of size: "+readerList==null? 0 : readerList.size());
      	  }
      	  final List<BoboIndexReader> boboReaders = ZoieIndexReader.extractDecoratedReaders(readerList);
      	  
      	  return SearchTimer.time(new Callable<Res>(){
      		public Res call() throws Exception{
      		  return handlePartitionedRequest(senseiReq,boboReaders,queryBuilderFactory);
      		}
      	  });
        }
        finally{
          if (readerFactory != null && readerList != null){
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
