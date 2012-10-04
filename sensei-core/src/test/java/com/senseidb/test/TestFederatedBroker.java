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
package com.senseidb.test;

import junit.framework.TestCase;

import org.apache.log4j.Logger;
import org.json.JSONObject;

import com.senseidb.search.req.mapred.TestMapReduce;
import com.senseidb.svc.api.SenseiService;

public class TestFederatedBroker extends TestCase {

  private static final Logger logger = Logger.getLogger(TestMapReduce.class);

  
  private static SenseiService httpRestSenseiService;
  static {
    SenseiStarter.start("test-conf/node1","test-conf/node2");     
    httpRestSenseiService = SenseiStarter.httpRestSenseiService;
  }
  public void test1OneClusterIsEnough() throws Exception { 
    String req = "{\"size\":10, \"query\":{\"path\":{\"makemodel\":\"asian/acura/3.2tl\"}}, \"selections\":[{\"range\":{\"year\":{\"to\":\"2002\",\"include_lower\":true,\"include_upper\":false,\"from\":\"2000\"}}}]}";
    JSONObject reqJson = new JSONObject(req);   
    JSONObject res = TestSensei.search(reqJson);
    assertEquals("numhits is wrong", 24, res.getInt("numhits"));  
    res = TestSensei.search(SenseiStarter.federatedBrokerUrl, reqJson.toString());
     assertEquals("numhits is wrong", 24, res.getInt("numhits"));
  }
  public void test2TwoClusters() throws Exception { 
    String req = "{\"size\":40, \"query\":{\"path\":{\"makemodel\":\"asian/acura/3.2tl\"}}, \"selections\":[{\"range\":{\"year\":{\"to\":\"2002\",\"include_lower\":true,\"include_upper\":false,\"from\":\"2000\"}}}]}";
    JSONObject reqJson = new JSONObject(req);   
    JSONObject res = TestSensei.search(reqJson);
    assertEquals("numhits is wrong", 24, res.getInt("numhits"));  
    res = TestSensei.search(SenseiStarter.federatedBrokerUrl, reqJson.toString());
     assertEquals("numhits is wrong", 48, res.getInt("numhits"));
     assertEquals("hits are wrong", 40, res.getJSONArray("hits").length());
  }

}
