package com.senseidb.test.client.json;

import com.senseidb.search.client.json.SenseiServiceProxy;
import com.senseidb.search.client.json.req.SenseiClientRequest;

public class EmptyRequest {
  public static void main(String[] args) {
    SenseiServiceProxy senseiServiceProxy = new SenseiServiceProxy("localhost", 8080);
    System.out.println(senseiServiceProxy.sendSearchRequest(SenseiClientRequest.builder().build()));
  }
}
