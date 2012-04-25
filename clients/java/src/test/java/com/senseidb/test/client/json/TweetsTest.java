package com.senseidb.test.client.json;

import com.senseidb.search.client.json.SenseiServiceProxy;
import com.senseidb.search.client.json.req.FacetInit;
import com.senseidb.search.client.json.req.FacetType;
import com.senseidb.search.client.json.req.Selection;
import com.senseidb.search.client.json.req.SenseiClientRequest;

public class TweetsTest {
public static void main(String[] args) throws Exception {

  SenseiClientRequest request = SenseiClientRequest.builder().addSelection(Selection.terms("timeRange", "000120000")).
      addFacetInit("timeRange", "time", FacetInit.build(FacetType.type_long, System.currentTimeMillis())).build();
  SenseiServiceProxy senseiServiceProxy = new SenseiServiceProxy("localhost", 8080);
  System.out.println(senseiServiceProxy.sendSearchRequest(request));
}
}
