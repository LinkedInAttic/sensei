package com.senseidb.federated.broker.proxy;

import java.util.Arrays;
import java.util.List;

import org.json.JSONObject;
import org.springframework.util.Assert;

import com.senseidb.search.node.inmemory.InMemorySenseiService;
import com.senseidb.search.req.SenseiRequest;
import com.senseidb.search.req.SenseiResult;

public class GenericBrokerProxy implements BrokerProxy {
  private InMemorySenseiService inMemorySenseiService;
  private ProxyDataSource proxyDataSource;
  private SenseiRequestFactory requestFactory;
  public GenericBrokerProxy(InMemorySenseiService inMemorySenseiService, ProxyDataSource proxyDataSource) {    
    Assert.notNull(inMemorySenseiService);
    Assert.notNull(proxyDataSource);
    this.inMemorySenseiService = inMemorySenseiService;
    this.proxyDataSource = proxyDataSource;
  }
  public GenericBrokerProxy() {
    
  }
  @Override
  public List<SenseiResult> doQuery(SenseiRequest senseiRequest) {
    List<JSONObject> documents = proxyDataSource.getData(senseiRequest);
    if (requestFactory != null) {
      senseiRequest = requestFactory.build(senseiRequest);
    }
    SenseiResult senseiResult = inMemorySenseiService.doQuery(senseiRequest, documents);
    return Arrays.asList(senseiResult);
  }
  public InMemorySenseiService getInMemorySenseiService() {
    return inMemorySenseiService;
  }
  public void setInMemorySenseiService(InMemorySenseiService inMemorySenseiService) {
    this.inMemorySenseiService = inMemorySenseiService;
  }
  public ProxyDataSource getProxyDataSource() {
    return proxyDataSource;
  }
  public void setProxyDataSource(ProxyDataSource proxyDataSource) {
    this.proxyDataSource = proxyDataSource;
  }
  public SenseiRequestFactory getRequestFactory() {
    return requestFactory;
  }
  public void setRequestFactory(SenseiRequestFactory requestFactory) {
    this.requestFactory = requestFactory;
  }
  
}
