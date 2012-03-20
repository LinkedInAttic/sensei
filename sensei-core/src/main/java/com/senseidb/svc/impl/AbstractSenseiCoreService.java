package com.senseidb.svc.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import org.apache.lucene.util.NamedThreadFactory;
import proj.zoie.api.IndexReaderFactory;
import proj.zoie.api.ZoieIndexReader;

import com.browseengine.bobo.api.BoboIndexReader;
import com.linkedin.norbert.network.Serializer;
import com.senseidb.metrics.MetricsConstants;
import com.senseidb.search.node.SenseiCore;
import com.senseidb.search.node.SenseiQueryBuilderFactory;
import com.senseidb.search.req.AbstractSenseiRequest;
import com.senseidb.search.req.AbstractSenseiResult;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.Timer;

public abstract class AbstractSenseiCoreService<Req extends AbstractSenseiRequest,Res extends AbstractSenseiResult>{
  private final static Logger logger = Logger.getLogger(AbstractSenseiCoreService.class);
  

  private static Timer GetReaderTimer = null;
  private static Timer SearchTimer = null;
  private static Timer MergeTimer = null;
  private static Meter SearchCounter = null;
	
  static{
	  // register jmx monitoring for timers
	  try{
	    MetricName getReaderMetricName = new MetricName(MetricsConstants.Domain,"timer","getreader-time","node");
	    GetReaderTimer = Metrics.newTimer(getReaderMetricName,TimeUnit.MILLISECONDS,TimeUnit.SECONDS);
	    
	    MetricName searchMetricName = new MetricName(MetricsConstants.Domain,"timer","search-time","node");
	    SearchTimer = Metrics.newTimer(searchMetricName,TimeUnit.MILLISECONDS,TimeUnit.SECONDS);

	    MetricName mergeMetricName = new MetricName(MetricsConstants.Domain,"timer","merge-time","node");
	    MergeTimer = Metrics.newTimer(mergeMetricName,TimeUnit.MILLISECONDS,TimeUnit.SECONDS);
	    
	    MetricName searchCounterMetricName = new MetricName(MetricsConstants.Domain,"meter","search-count","node");
	    SearchCounter = Metrics.newMeter(searchCounterMetricName, "requets", TimeUnit.SECONDS);

	  }
	  catch(Exception e){
		logger.error(e.getMessage(),e);
	  }
  }
  protected long _timeout = 8000;
    
  protected final SenseiCore _core;
  
  private final NamedThreadFactory threadFactory = new NamedThreadFactory("parallel-searcher");
  private final ExecutorService _executorService = Executors.newCachedThreadPool(threadFactory);
  
  private final Map<Integer,Timer> partitionTimerMetricMap = new HashMap<Integer,Timer>();
	
	public AbstractSenseiCoreService(SenseiCore core){
	  _core = core;
	  int[] partitions = _core.getPartitions();
	  
    for (int partition : partitions){
      MetricName partitionSearchMetricName = new MetricName(MetricsConstants.Domain,"timer","partition-time-"+partition,"partition");
      Timer timer = Metrics.newTimer(partitionSearchMetricName,TimeUnit.MILLISECONDS,TimeUnit.SECONDS);
      partitionTimerMetricMap.put(partition, timer); 
    }
	}
	
	public final Res execute(final Req senseiReq){
		SearchCounter.mark();
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
                  Timer timer = partitionTimerMetricMap.get(partition);
                  Res res = timer.time(new Callable<Res>(){

                    @Override
                    public Res call() throws Exception {
                      return  handleRequest(senseiReq, readerFactory, _core.getQueryBuilderFactory());
                    }                    
                  });
                  
                  long end = System.currentTimeMillis();
                  res.setTime(end - start);
                  
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
              Timer timer = partitionTimerMetricMap.get(partition);
              Res res = timer.time(new Callable<Res>(){

                @Override
                public Res call() throws Exception {
                  return  handleRequest(senseiReq, readerFactory, _core.getQueryBuilderFactory());
                }                    
              });
              
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
            if (readerFactory == null)
              return Collections.EMPTY_LIST;
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

	public abstract Serializer<Req, Res> getSerializer();
}
