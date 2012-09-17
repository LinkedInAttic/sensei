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

import java.util.ArrayList;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import voldemort.client.StoreClient;

import com.senseidb.federated.broker.proxy.ProxyDataSource;
import com.senseidb.search.req.SenseiRequest;

public class DefaultVoldermortDataSource implements ProxyDataSource {
  private StoreClient<String, String> storeClient;
  private String key;


  @Override
  public List<JSONObject> getData(SenseiRequest senseiRequest) {
    String raw =  storeClient.get(key).getValue();   
    try {
      JSONArray jsonArray = new JSONArray(raw);
      List<JSONObject> ret = new ArrayList<JSONObject>(jsonArray.length());
      for (int i = 0; i< jsonArray.length(); i++) {
          ret.add(jsonArray.getJSONObject(i));
      }
    return ret;
    } catch (JSONException e) {
      throw new RuntimeException(e);
    }
  }
  
  public StoreClient<String, String> getStoreClient() {
    return storeClient;
  }
  public void setStoreClient(StoreClient<String, String> storeClient) {
    this.storeClient = storeClient;
  }
  public String getKey() {
    return key;
  }
  public void setKey(String key) {
    this.key = key;
  }
}
