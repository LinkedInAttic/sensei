package com.senseidb.test.client;

import com.senseidb.search.client.SenseiServiceProxy;
import com.senseidb.search.client.req.SenseiClientRequest;

public class EmptyRequest {
  public static void main(String[] args) {
    SenseiServiceProxy senseiServiceProxy = new SenseiServiceProxy("localhost", 8080);
    System.out.println(senseiServiceProxy.sendSearchRequest(SenseiClientRequest.builder().build()));
  }
}
