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
