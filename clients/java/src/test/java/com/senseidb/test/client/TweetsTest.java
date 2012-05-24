package com.senseidb.test.client;

import com.senseidb.search.client.SenseiServiceProxy;
import com.senseidb.search.client.req.FacetInit;
import com.senseidb.search.client.req.FacetType;
import com.senseidb.search.client.req.Selection;
import com.senseidb.search.client.req.SenseiClientRequest;
import org.junit.Test;

public class TweetsTest {
public static void main(String[] args) throws Exception {

  SenseiClientRequest request = SenseiClientRequest.builder().addSelection(Selection.terms("timeRange", "000120000")).
      addFacetInit("timeRange", "time", FacetInit.build(FacetType.type_long, System.currentTimeMillis())).build();
  SenseiServiceProxy senseiServiceProxy = new SenseiServiceProxy("localhost", 8080);
  System.out.println(senseiServiceProxy.sendSearchRequest(request));
}

  @Test
  public void bareEntry() throws Exception
  {
  }

}
