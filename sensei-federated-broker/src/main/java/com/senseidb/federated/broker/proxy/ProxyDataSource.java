package com.senseidb.federated.broker.proxy;

import java.util.List;

import org.json.JSONObject;

import com.senseidb.search.req.SenseiRequest;

public interface ProxyDataSource {
  public List<JSONObject> getData(SenseiRequest senseiRequest);
}
