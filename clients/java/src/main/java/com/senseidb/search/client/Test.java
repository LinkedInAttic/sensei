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

package com.senseidb.search.client;

import org.json.JSONObject;

import com.senseidb.search.client.json.JsonSerializer;
import com.senseidb.search.client.req.Facet;
import com.senseidb.search.client.req.Selection;
import com.senseidb.search.client.req.SenseiClientRequest;

public class Test {
  public static void main(String[] args) throws Exception {
    SenseiServiceProxy senseiServiceProxy = new SenseiServiceProxy("localhost", 8080);
    //SenseiClientRequest clientRequest = SenseiClientRequest.builder().addFacet("account_id", Facet.builder().minHit(0).max(200).build()).build();
    SenseiClientRequest clientRequest = SenseiClientRequest.builder().filter(Selection.terms("account_id", "1139")).paging(1000, 10).addFacet("account_id", Facet.builder().minHit(0).max(200).build()).build();
    
    
    JSONObject json = (JSONObject) JsonSerializer.serialize(clientRequest);
    JSONObject mapReduce = new JSONObject().put("function", "sensei.distinctCount").put("parameters", new JSONObject().put("column", "account_id"));
    json.put("mapReduce", mapReduce);
    System.out.println(new JSONObject(senseiServiceProxy.sendPostRaw(senseiServiceProxy.getSearchUrl(), json.toString())).toString(1));
  }
}
