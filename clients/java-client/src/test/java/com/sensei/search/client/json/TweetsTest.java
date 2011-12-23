package com.sensei.search.client.json;

import com.sensei.search.client.json.req.FacetInit;
import com.sensei.search.client.json.req.Selection;
import com.sensei.search.client.json.req.SenseiClientRequest;

public class TweetsTest {
public static void main(String[] args) throws Exception {

  SenseiClientRequest request = SenseiClientRequest.builder().addSelection(Selection.terms("timeRange", "000120000")).
      addFacetInit("timeRange", "time", FacetInit.build("long", System.currentTimeMillis())).build();
  SenseiServiceProxy senseiServiceProxy = new SenseiServiceProxy("localhost", 8080);
  System.out.println(senseiServiceProxy.sendSearchRequest(request));
}
}
