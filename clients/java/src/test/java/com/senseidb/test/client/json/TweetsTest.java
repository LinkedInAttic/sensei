package com.senseidb.test.client.json;

import com.senseidb.search.client.json.SenseiServiceProxy;
import com.senseidb.search.client.json.req.FacetInit;
import com.senseidb.search.client.json.req.Selection;
import com.senseidb.search.client.json.req.SenseiClientRequest;
import org.junit.Test;

public class TweetsTest {
public static void main(String[] args) throws Exception {

  SenseiClientRequest request = SenseiClientRequest.builder().addSelection(Selection.terms("timeRange", "000120000")).
      addFacetInit("timeRange", "time", FacetInit.build("long", System.currentTimeMillis())).build();
  SenseiServiceProxy senseiServiceProxy = new SenseiServiceProxy("localhost", 8080);
  System.out.println(senseiServiceProxy.sendSearchRequest(request));
}

  @Test
  public void bareEntry() throws Exception
  {
  }

}
