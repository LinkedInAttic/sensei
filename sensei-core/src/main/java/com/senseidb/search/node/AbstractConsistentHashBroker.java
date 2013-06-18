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
package com.senseidb.search.node;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.senseidb.metrics.MetricName;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.log4j.Logger;

import com.linkedin.norbert.NorbertException;
import com.linkedin.norbert.javacompat.cluster.Node;
import com.linkedin.norbert.javacompat.network.PartitionedNetworkClient;
import com.linkedin.norbert.javacompat.network.RequestBuilder;
import com.linkedin.norbert.network.ResponseIterator;
import com.linkedin.norbert.network.Serializer;
import com.linkedin.norbert.network.common.ExceptionIterator;
import com.linkedin.norbert.network.common.PartialIterator;
import com.linkedin.norbert.network.common.TimeoutIterator;
import com.senseidb.metrics.MetricFactory;
import com.senseidb.search.req.AbstractSenseiRequest;
import com.senseidb.search.req.AbstractSenseiResult;
import com.senseidb.search.req.ErrorType;
import com.senseidb.search.req.SenseiError;
import com.senseidb.search.req.SenseiRequest;
import com.senseidb.svc.api.SenseiException;

/**
 * @author "Xiaoyang Gu<xgu@linkedin.com>"
 *
 * @param <REQUEST>
 * @param <RESULT>
 */
public abstract class AbstractConsistentHashBroker<REQUEST extends AbstractSenseiRequest, RESULT extends AbstractSenseiResult>
    extends AbstractSenseiBroker<REQUEST, RESULT>
{
  private final static Logger logger = Logger.getLogger(AbstractConsistentHashBroker.class);

  protected long _timeout = 8000;
  protected final Serializer<REQUEST, RESULT> _serializer;

  private final Timer _scatterTimer;
  private final Timer _gatherTimer;
  private final Timer _totalTimer;
  private final Meter _searchCounter;
  private final Meter _errorMeter;
  private final Meter _emptyMeter;
  
  /**
   * @param networkClient
   * @param serializer
   *          The serializer used to serialize/deserialize request/response pairs
   * @throws NorbertException
   */
  public AbstractConsistentHashBroker(PartitionedNetworkClient<String> networkClient, Serializer<REQUEST, RESULT> serializer)
      throws NorbertException
  {
    super(networkClient);
    _serializer = serializer;

    // register metrics monitoring for timers
    MetricName scatterMetricName = new MetricName("scatter-time","broker");
    _scatterTimer = MetricFactory.newTimer(scatterMetricName);

    MetricName gatherMetricName = new MetricName("gather-time","broker");
    _gatherTimer = MetricFactory.newTimer(gatherMetricName);

    MetricName totalMetricName = new MetricName("total-time","broker");
    _totalTimer = MetricFactory.newTimer(totalMetricName);

    MetricName searchCounterMetricName = new MetricName("search-count","broker");
    _searchCounter = MetricFactory.newMeter(searchCounterMetricName);

    MetricName errorMetricName = new MetricName("error-meter","broker");
    _errorMeter = MetricFactory.newMeter(errorMetricName);

    MetricName emptyMetricName = new MetricName("empty-meter","broker");
    _emptyMeter = MetricFactory.newMeter(emptyMetricName);
  }

	public <T> T customizeRequest(REQUEST request)
	{
		return (T) request;
	}

  protected IntSet getPartitions(Set<Node> nodes)
  {
	    IntSet partitionSet = new IntOpenHashSet();
	    for (Node n : nodes)
	    {
	      partitionSet.addAll(n.getPartitionIds());
	    }
	    return partitionSet;
	  }

  /**
   * @return an empty result instance. Used when the request cannot be properly
   *         processed or when the true result is empty.
   */
  public abstract RESULT getEmptyResultInstance();

  /**
   * The method that provides the search service.
   * 
   * @param req
   * @return
   * @throws SenseiException
   */
  public RESULT browse(final REQUEST req) throws SenseiException
  {
//    if (_partitions == null){
//      ErrorMeter.mark();
//      throw new SenseiException("Browse called before cluster is connected!");
//    }
    _searchCounter.mark();
    try
    {
      return _totalTimer.time(new Callable<RESULT>(){
    	@Override
  		public RESULT call() throws Exception {
          return doBrowse(_networkClient, req, _partitions); 	  
    	}
      });
    } 
    catch (Exception e)
    {
      _errorMeter.mark();
      throw new SenseiException(e.getMessage(), e);
    }
  }

  /**
   * Merge results on the client/broker side. It likely works differently from
   * the one in the search node.
   * 
   * @param request
   *          the original request object
   * @param resultList
   *          the list of results from all the requested partitions.
   * @return one single result instance that is merged from the result list.
   */
  public abstract RESULT mergeResults(REQUEST request, List<RESULT> resultList);

  protected String getRouteParam(REQUEST req) {
    String param = req.getRouteParam();
    if (param == null) {
      return RandomStringUtils.random(4);
    }
    else {
      return param;
    }
  }

  protected RESULT doBrowse(PartitionedNetworkClient<String> networkClient, final REQUEST req, IntSet partitions)
  {
    final long time = System.currentTimeMillis();

    final List<RESULT> resultList = new ArrayList<RESULT>();
   
    try {
      resultList.addAll(_scatterTimer.time(new Callable<List<RESULT>>()
      {
        @Override
        public List<RESULT> call()
            throws Exception
        {
          return doCall(req);
        }
      }));
    } catch (Exception e) {
      _errorMeter.mark();
      RESULT emptyResult = getEmptyResultInstance();
      logger.error("Error running scatter/gather", e);
      emptyResult.addError(new SenseiError("Error gathering the results" + e.getMessage(), ErrorType.BrokerGatherError));
      return emptyResult;
    }

    if (resultList.size() == 0)
    {
      logger.error("no result received at all return empty result");
      RESULT emptyResult = getEmptyResultInstance();
      emptyResult.addError(new SenseiError("Error gathering the results. " + "no result received at all return empty result", ErrorType.BrokerGatherError));
      _emptyMeter.mark();
      return emptyResult;
    }

    RESULT result = null;
    try {
      result = _gatherTimer.time(new Callable<RESULT>() {
        @Override
        public RESULT call() throws Exception {
          return mergeResults(req, resultList);
        }
      });
    } catch (Exception e) {
      result = getEmptyResultInstance();
      logger.error("Error gathering the results", e);
      result.addError(new SenseiError("Error gathering the results" + e.getMessage(), ErrorType.BrokerGatherError));
      _errorMeter.mark();
    }

    if (logger.isDebugEnabled()){
      logger.debug("remote search took " + (System.currentTimeMillis() - time) + "ms");
    }

    return result;
  }

  protected List<RESULT> doCall(final REQUEST req) throws ExecutionException {
    List<RESULT> resultList = new ArrayList<RESULT>();

    // only instantiate if debug logging is enabled
    final List<StringBuilder> timingLogLines = logger.isDebugEnabled() ? new LinkedList<StringBuilder>() : null;
    
    ResponseIterator<RESULT> responseIterator =
        buildIterator(_networkClient.sendRequestToOneReplica(getRouteParam(req), new RequestBuilder<Integer, REQUEST>() {
          @Override
          public REQUEST apply(Node node, Set<Integer> nodePartitions) {
            // TODO: Cloning is yucky per http://www.artima.com/intv/bloch13.html
            REQUEST clone = (REQUEST) (((SenseiRequest) req).clone());
            
            clone.setPartitions(nodePartitions);
            if (timingLogLines != null) {
              // this means debug logging was enabled, produce first portion of log lines
              timingLogLines.add(buildLogLineForRequest(node, clone));
            }
  
            REQUEST customizedRequest = customizeRequest(clone);
            return customizedRequest;
          }
        }, _serializer));

    while(responseIterator.hasNext()) {
      resultList.add(responseIterator.next());
    }

    if (timingLogLines != null) {
      // this means debug logging was enabled, complete the timing log lines and log them
      int i = 0;
      for (StringBuilder logLine : timingLogLines) {
        // we are assuming the request builder gets called in the same order as the response
        // iterator is built, otherwise the loglines would be out of sync between req & res
        if (i < resultList.size()) {
          logger.debug(buildLogLineForResult(logLine, resultList.get(i++)));
        }
      }
      logger.debug(String.format("There are %d responses", resultList.size()));
    }

    return resultList;
  }

  protected StringBuilder buildLogLineForRequest(Node node, REQUEST clone) {
    return new StringBuilder()
        .append("Request to individual node - id:")
        .append(node.getId())
        .append(" - url:")
        .append(node.getUrl())
        .append(" - partitions:")
        .append(node.getPartitionIds());
  }
  
  protected StringBuilder buildLogLineForResult(StringBuilder logLine, RESULT result) {
    return logLine.append(" - took ").append(result.getTime()).append("ms.");
  }
  
  protected ResponseIterator<RESULT> buildIterator(ResponseIterator<RESULT> responseIterator) {
    TimeoutIterator<RESULT> timeoutIterator = new TimeoutIterator<RESULT>(responseIterator, _timeout);
    if(allowPartialMerge()) {
      return new PartialIterator<RESULT>(new ExceptionIterator<RESULT>(timeoutIterator));
    }
    return timeoutIterator;
  }

  public void shutdown()
  {
    logger.info("shutting down broker...");
  }

  public long getTimeout() {
    return _timeout;
  }

  public void setTimeout(long timeout) {
    this._timeout = timeout;
  }

  /**
   * @return boolean representing whether or not the server can tolerate node failures or timeouts and merge the other
   * results. It's a tradeoff between fault tolerance and accuracy that may be acceptable for some applications
   */
  public abstract boolean allowPartialMerge();
}
