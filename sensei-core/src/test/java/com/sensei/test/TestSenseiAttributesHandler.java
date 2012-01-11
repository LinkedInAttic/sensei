package com.sensei.test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;

import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import com.browseengine.bobo.api.BrowseFacet;
import com.browseengine.bobo.api.FacetSpec;
import com.browseengine.bobo.api.FacetSpec.FacetSortSpec;
import com.sensei.search.nodes.SenseiBroker;
import com.sensei.search.req.SenseiRequest;
import com.sensei.search.req.SenseiResult;
import com.sensei.search.svc.api.SenseiService;

public class TestSenseiAttributesHandler extends TestCase {

  private static final Logger logger = Logger.getLogger(TestSenseiAttributesHandler.class);

  private static SenseiBroker broker;
  private static SenseiService httpRestSenseiService;
  static {
    SenseiStarter.start("test-conf/node1","test-conf/node2");
    broker = SenseiStarter.broker;
    httpRestSenseiService = SenseiStarter.httpRestSenseiService;

  }

  public void testSelectionDynamicTimeRangeJson() throws Exception
  {
   // Thread.sleep(2000);
    logger.info("executing test case Selection terms");
    String req = "{"+
        "\"facets\":{    \"object_properties\":{\"minHit\":1 }}" +
        "}";
    System.out.println(req);
    JSONObject res = search(new JSONObject(req));
    System.out.println(res.toString(1));
    assertEquals("numhits is wrong", 15000, res.getInt("numhits"));
    assertTrue(res.getJSONObject("facets").getJSONArray("object_properties").length() > 5);
  }
  public void testTotalCountWithFacetSpec() throws Exception
  {
    logger.info("executing test case testTotalCountWithFacetSpec");
    SenseiRequest req = new SenseiRequest();
    
    FacetSpec facetSpec = new FacetSpec();
    facetSpec.setMaxCount(30);
    facetSpec.setOrderBy(FacetSortSpec.OrderHitsDesc);
    setspec(req, facetSpec);
    req.setCount(5);
    req.setFacetSpec("object_properties", facetSpec);
    //setspec(req, facetSpecall);
    SenseiResult res = broker.browse(req);
    List<BrowseFacet> facets = res.getFacetAccessor("object_properties").getFacets();
    assertEquals(facets.size(), 30);
    
  
  }
  public void testAttributesFacetHandlerTwoTermsAndOneExcluded() throws Exception
  {
   //Thread.sleep(1000L);
    logger.info("executing test case Selection terms");
    String req = "{\"selections\":[{\"terms\":{\"object_properties\":{\"values\":[\"key1\"],\"operator\":\"or\"}}}]" +
    		//",\"facets\":{    \"object_properties\":{\"minHit\":1 }}" +
    		"}";
    assertEquals(broker.browse(new SenseiRequest()).getNumHits(), 15000);
    JSONObject res = search(new JSONObject(req));
    assertEquals("numhits is wrong", 5420, res.getInt("numhits"));
  }
   
  
  private JSONObject search(JSONObject req) throws Exception  {
    return  search(SenseiStarter.SenseiUrl, req.toString());
  }
  private JSONObject searchGet(JSONArray req) throws Exception  {
    return  search(new URL(SenseiStarter.SenseiUrl.toString() + "/get"), req.toString());
  }
  private JSONObject search(URL url, String req) throws Exception {
    URLConnection conn = url.openConnection();
    conn.setDoOutput(true);
    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(conn.getOutputStream(), "UTF-8"));
    String reqStr = req;
    System.out.println("req: " + reqStr);
    writer.write(reqStr, 0, reqStr.length());
    writer.flush();
    BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream(), "UTF-8"));
    StringBuilder sb = new StringBuilder();
    String line = null;
    while((line = reader.readLine()) != null)
      sb.append(line);
    String res = sb.toString();
    System.out.println("res: " + res);
    return new JSONObject(res);
  }

  private void setspec(SenseiRequest req, FacetSpec spec) {
    req.setFacetSpec("tags_attributes", spec);
   
  }








  private void checkColorOrder(ArrayList<String> arColors)
  {
    assertTrue("must have 15000 results, size is:" + arColors.size(), arColors.size() == 15000);
    for(int i=0; i< arColors.size()-1; i++){
      String first = arColors.get(i);
      String next = arColors.get(i+1);
      int comp = first.compareTo(next);
      assertTrue("should >=0 (first= "+ first+"  next= "+ next+")", comp>=0);
    }
  }


}
