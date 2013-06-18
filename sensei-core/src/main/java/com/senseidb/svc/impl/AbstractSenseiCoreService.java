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

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.senseidb.metrics.MetricFactory;
import com.senseidb.metrics.MetricName;
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
import java.util.concurrent.TimeoutException;

import org.apache.log4j.Logger;

import org.apache.lucene.util.NamedThreadFactory;
import proj.zoie.api.IndexReaderFactory;
import proj.zoie.api.ZoieIndexReader;

import com.browseengine.bobo.api.BoboIndexReader;
import com.linkedin.norbert.network.Serializer;
import com.senseidb.search.node.SenseiCore;
import com.senseidb.search.node.SenseiQueryBuilderFactory;
import com.senseidb.search.req.AbstractSenseiRequest;
import com.senseidb.search.req.AbstractSenseiResult;
import com.senseidb.search.req.ErrorType;
import com.senseidb.search.req.SenseiError;

public abstract class AbstractSenseiCoreService<Req extends AbstractSenseiRequest,Res extends AbstractSenseiResult>{
  private final static Logger logger = Logger.getLogger(AbstractSenseiCoreService.class);
  

  private final Timer _getReaderTimer;
  private final Timer _searchTimer;
  private final Timer _mergeTimer;
  private final Meter _searchCounter;

  protected long _timeout = 8000;
    
  protected final SenseiCore _core;
  
  private final NamedThreadFactory threadFactory = new NamedThreadFactory("parallel-searcher");
  private final ExecutorService _executorService = Executors.newCachedThreadPool(threadFactory);
  
  private final Map<Integer,Timer> partitionTimerMetricMap = new HashMap<Integer,Timer>();
	
	public AbstractSenseiCoreService(SenseiCore core){
	  _core = core;
    _getReaderTimer = registerTimer("getreader-time");
    _searchTimer = registerTimer("search-time");
    _mergeTimer = registerTimer("merge-time");
    _searchCounter = registerMeter("search-count");
	}
  
  private Timer buildTimer(int partition) {
    MetricName partitionSearchMetricName = new MetricName("partition-time-"+partition,"partition");
    return MetricFactory.newTimer(partitionSearchMetricName);
  }
  
  private Timer getTimer(int partition) {
    Timer timer = partitionTimerMetricMap.get(partition);
    if(timer == null) {
      partitionTimerMetricMap.put(partition, buildTimer(partition));
      return getTimer(partition);
    }
    return timer;
  }
	
	public final Res execute(final Req senseiReq){
		_searchCounter.mark();
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

          if (i < partitions.size() - 1)  // Search simultaneously.
          {
            try
            {
              futures[i] = (Future<Res>)_executorService.submit(new Callable<Res>()
              {
                public Res call() throws Exception
                {
                  Timer timer = getTimer(partition);
                  
                  Res res = timer.time(new Callable<Res>(){

                    @Override
                    public Res call() throws Exception {
                      return  handleRequest(senseiReq, readerFactory, _core.getQueryBuilderFactory());
                    }                    
                  });
                  
                  long end = System.currentTimeMillis();
                  res.setTime(end - start);
                  logger.info("searching partition: " + partition + " browse took: " + res.getTime());

                  return res;
                }
              });
            } catch (Exception e)
            {
              senseiReq.addError(new SenseiError(e.getMessage(), ErrorType.BoboExecutionError));              
              logger.error(e.getMessage(), e);
            }
          }
          else  // Reuse current thread.
          {
            try
            {
              Timer timer = getTimer(partition);
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
              senseiReq.addError(new SenseiError(e.getMessage(), ErrorType.BoboExecutionError));       
              
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
	          if (e instanceof TimeoutException) {
	            senseiReq.addError(new SenseiError(e.getMessage(), ErrorType.ExecutionTimeout));    
	          } else {
	            senseiReq.addError(new SenseiError(e.getMessage(), ErrorType.BoboExecutionError));       
	          }
	          resultList.add(getEmptyResultInstance(e));
          }
        }

          try{
	        finalResult = _mergeTimer.time(new Callable<Res>(){
	    	 public Res call() throws Exception{
	    	   return mergePartitionedResults(senseiReq, resultList);
	    	 }
	        });
          }
          catch(Exception e){
        	logger.error(e.getMessage(),e);
        	finalResult = getEmptyResultInstance(null);
        	finalResult.addError(new SenseiError(e.getMessage(), ErrorType.MergePartitionError));
          }
	    } 
	    else
	    {
	      if (logger.isInfoEnabled()){
	        logger.info("no partitions specified");
	      }
	      finalResult = getEmptyResultInstance(null);
	      finalResult.addError(new SenseiError("no partitions specified", ErrorType.PartitionCallError));
	    }
	    if (logger.isInfoEnabled()){
	      logger.info("searching partitions  " + String.valueOf(partitions) + " took: " + finalResult.getTime());
	    }
	    return finalResult;
	}
	
	private final Res handleRequest(final Req senseiReq,final IndexReaderFactory<ZoieIndexReader<BoboIndexReader>> readerFactory,final SenseiQueryBuilderFactory queryBuilderFactory) throws Exception{
		List<ZoieIndexReader<BoboIndexReader>> readerList = null;
        try{
      	  readerList = _getReaderTimer.time(new Callable<List<ZoieIndexReader<BoboIndexReader>>>(){
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
      	  
      	  return _searchTimer.time(new Callable<Res>(){
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

  protected final Timer registerTimer(String name)
  {
    return MetricFactory.newTimer(new MetricName(name, getMetricScope()));
  }

  protected final Meter registerMeter(String name)
  {
    return MetricFactory.newMeter(new MetricName(name, getMetricScope()));
  }
	
	public abstract Res handlePartitionedRequest(Req r,final List<BoboIndexReader> readerList,SenseiQueryBuilderFactory queryBuilderFactory) throws Exception;
	public abstract Res mergePartitionedResults(Req r,List<Res> reqList);
	public abstract Res getEmptyResultInstance(Throwable error);

	public abstract Serializer<Req, Res> getSerializer();

  /**
   * Returns the name of the metric scope. It's used for creating {@link MetricName} that get registered through
   * {@link MetricFactory}.
   */
  protected abstract String getMetricScope();
}
