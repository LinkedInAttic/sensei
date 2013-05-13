package com.senseidb.federated.broker;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.json.JSONObject;

import com.senseidb.federated.broker.proxy.BrokerProxy;
import com.senseidb.federated.broker.proxy.SenseiBrokerProxy;
import com.senseidb.search.node.Broker;
import com.senseidb.search.node.ResultMerger;
import com.senseidb.search.node.SenseiBroker;
import com.senseidb.search.node.inmemory.InMemorySenseiService;
import com.senseidb.search.req.SenseiRequest;
import com.senseidb.search.req.SenseiResult;
import com.senseidb.search.req.SenseiSystemInfo;
import com.senseidb.servlet.AbstractSenseiClientServlet;
import com.senseidb.servlet.DefaultSenseiJSONServlet;
import com.senseidb.svc.api.SenseiException;
import com.senseidb.util.RequestConverter2;

import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_RESULT_NUMHITS;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_RESULT_TOTALDOCS;

public class FederatedBroker implements Broker<SenseiRequest, SenseiResult>{
  private final static Logger logger = Logger.getLogger(SenseiBrokerProxy.class);
  private final static Logger queryLogger = Logger.getLogger("com.sensei.querylog");
  private List<BrokerProxy> proxies;
  private int numThreads = 10;  
  private ExecutorService executor;
  private long timeout = 8000;
  
  private Map<String, String[]> facetInfo = new HashMap<String, String[]>();
  public FederatedBroker() {
  }
  public FederatedBroker(List<BrokerProxy> proxies) {
    this.proxies = proxies;
  }
  public void start() {
    executor = Executors.newFixedThreadPool(numThreads);
  }
  
  public List<BrokerProxy> getProxies() {
    return proxies;
  }
  public void setProxies(List<BrokerProxy> proxies) {
    this.proxies = proxies;
  }
  
  public int getNumThreads() {
    return numThreads;
  }
  public void setNumThreads(int numThreads) {
    this.numThreads = numThreads;
  }
  
  
  public long getTimeout() {
    return timeout;
  }
  public void setTimeout(long timeout) {
    this.timeout = timeout;
  }
  @Override
  public SenseiResult browse(final SenseiRequest request) throws SenseiException {
    final List<SenseiResult> resultList = Collections.synchronizedList(new ArrayList<SenseiResult>());
    final CountDownLatch countDownLatch = new CountDownLatch(proxies.size());
    for (final BrokerProxy proxy : proxies) {
      executor.submit(new Runnable() {        
        public void run() {
          try {
            resultList.addAll(proxy.doQuery(request));
            countDownLatch.countDown();
          } catch (Exception ex) {
            logger.error("Error while calling the proxy", ex);
          }
        }
      });      
    }
    try {
      boolean allTheResults = countDownLatch.await(timeout, TimeUnit.MILLISECONDS);
      if (!allTheResults) {        
        logger.warn("Not all the results are received");
      }
      SenseiResult res = ResultMerger.merge(request, resultList, false);
      // get src data (fetchStored) even if request doesn't have it could be enabled at
      // individual broker level
      SenseiBroker.recoverSrcData(res, res.getSenseiHits(), true);
      return res;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }  
  }
  
 
  public void setInMemorySenseiService(InMemorySenseiService inMemorySenseiService) {
    if (inMemorySenseiService != null && inMemorySenseiService.getSenseiSystemInfo() != null) {
      facetInfo = AbstractSenseiClientServlet.extractFacetInfo(inMemorySenseiService.getSenseiSystemInfo());      
    }
  }
  public JSONObject query(JSONObject request) {
    try {
      long time = System.currentTimeMillis();
      int numHits = 0, totalDocs = 0;
      try {
        SenseiRequest senseiRequest = RequestConverter2.fromJSON(request, facetInfo);
        SenseiResult senseiResult = browse(senseiRequest);
        JSONObject jsonResult = DefaultSenseiJSONServlet.buildJSONResult(senseiRequest, senseiResult);
        if (jsonResult != null) {
          numHits = jsonResult.optInt(PARAM_RESULT_NUMHITS);
          totalDocs = jsonResult.optInt(PARAM_RESULT_TOTALDOCS);
        }
        return jsonResult;
      }
      finally {
        if (queryLogger.isInfoEnabled() && request != null) {
          queryLogger.info(String.format("hits(%d/%d) took %dms: %s", numHits, totalDocs, System.currentTimeMillis() - time, request.toString()));
        }
      }
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  } 
  public void stop() {
    executor.shutdown();
  }
}
