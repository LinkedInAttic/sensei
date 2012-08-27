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

package com.senseidb.federated.broker;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.browseengine.bobo.api.BrowseSelection;
import com.senseidb.federated.broker.proxy.BrokerProxy;
import com.senseidb.search.req.SenseiRequest;
import com.senseidb.search.req.SenseiResult;

import voldemort.client.StoreClient;

public class FederatedBrokerIntegrationTest extends TestCase {
  
  private ClassPathXmlApplicationContext brokerContext;
  private FederatedBroker federatedBroker;
  private StoreClient<String, String> storeClient;
  private BrokerProxy senseiProxy;
  @Override
  protected void setUp() throws Exception {
   SingleNodeStarter.start("conf", 15000);
   brokerContext = new ClassPathXmlApplicationContext("federatedBroker-context.xml");
   federatedBroker = (FederatedBroker) brokerContext.getBean("federatedBroker", FederatedBroker.class);
   storeClient = (StoreClient<String,String>) brokerContext.getBean("storeClient");
   senseiProxy = (BrokerProxy) brokerContext.getBean("senseiProxy");
   JSONArray arr = readCarDocs();
   storeClient.put("test", arr.toString());
  }
  private JSONArray readCarDocs() throws IOException, URISyntaxException, JSONException {
    JSONArray arr = new JSONArray();
     LineIterator lineIterator = FileUtils.lineIterator(new File(FederatedBrokerIntegrationTest.class.getClassLoader().getResource("data/cars.json").toURI()));
     while(lineIterator.hasNext()) {
       String car = lineIterator.next();
       if (car != null && car.contains("{")) {
        JSONObject carDoc = new JSONObject(car);
        carDoc.put("id", carDoc.getLong("id") + 15000);
        arr.put(carDoc);
      }
      
     }
    return arr;
  }
  public void test1SearchOnTwoClusters() throws Exception {
    SenseiRequest req = new SenseiRequest();
    BrowseSelection sel = new BrowseSelection("year");
    String selVal = "[2001 TO 2002]";
    sel.addValue(selVal);
    req .addSelection(sel);
    SenseiResult result = federatedBroker.browse(req);
    assertEquals(30000, result.getTotalDocs());
    assertEquals(5814, result.getNumHits());
    SenseiResult oneProxyResult = senseiProxy.doQuery(req).get(0);
    assertEquals(15000, oneProxyResult.getTotalDocs());
    assertEquals(2907, oneProxyResult.getNumHits());
    
  }
  
  
  @Override
  protected void tearDown() throws Exception {
    brokerContext.close();
  }
}
