package com.sensei.search.client.json;

import com.sensei.search.client.json.req.SenseiClientRequest;

public class EmptyRequest {
  public static void main(String[] args) {
    SenseiServiceProxy senseiServiceProxy = new SenseiServiceProxy("localhost", 8080);
    System.out.println(senseiServiceProxy.sendSearchRequest(SenseiClientRequest.builder().build()));
  }
}
