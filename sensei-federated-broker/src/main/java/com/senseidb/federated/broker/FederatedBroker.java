package com.senseidb.federated.broker;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.senseidb.federated.broker.proxy.BrokerProxy;
import com.senseidb.federated.broker.proxy.SenseiBrokerProxy;
import com.senseidb.search.node.Broker;
import com.senseidb.search.node.ResultMerger;
import com.senseidb.search.node.SenseiBroker;
import com.senseidb.search.req.SenseiRequest;
import com.senseidb.search.req.SenseiResult;
import com.senseidb.svc.api.SenseiException;

public class FederatedBroker implements Broker<SenseiRequest, SenseiResult>{
  private final static Logger logger = Logger.getLogger(SenseiBrokerProxy.class);
  private List<BrokerProxy> proxies;
  private int numThreads = 10;  
  private ExecutorService executor;
  private long timeout = 8000;
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
      if (request.isFetchStoredFields() || request.isFetchStoredValue())
        SenseiBroker.recoverSrcData(res, res.getSenseiHits(), request.isFetchStoredFields());
      return res;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }  
  }
  public void stop() {
    executor.shutdown();
  }
}
