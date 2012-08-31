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
