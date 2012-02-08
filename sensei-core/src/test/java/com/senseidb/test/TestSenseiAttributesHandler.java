package com.senseidb.test;

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
import org.json.JSONObject;

import com.browseengine.bobo.api.BrowseFacet;
import com.browseengine.bobo.api.FacetSpec;
import com.browseengine.bobo.api.FacetSpec.FacetSortSpec;
import com.browseengine.bobo.facets.attribute.AttributesFacetHandler;
import com.senseidb.search.node.SenseiBroker;
import com.senseidb.search.req.SenseiRequest;
import com.senseidb.search.req.SenseiResult;
import com.senseidb.svc.api.SenseiService;


public class TestSenseiAttributesHandler extends TestCase {

  private static final Logger logger = Logger.getLogger(TestSenseiAttributesHandler.class);

  private static SenseiBroker broker;
  private static SenseiService httpRestSenseiService;
  static {
    SenseiStarter.start("test-conf/node1","test-conf/node2");
    broker = SenseiStarter.broker;
    httpRestSenseiService = SenseiStarter.httpRestSenseiService;

  }

  public void test1aMultiRangeHandler() throws Exception
  {
   
    logger.info("executing test case Selection terms");
    String req = "{\"selections\":[{\"terms\":{\"groupid_range_multi\":{\"values\":[\"[-300 TO -1]\", \"[1 TO 1000]\"],\"operator\":\"or\"}}}]" +
        //",\"facets\":{    \"object_properties\":{\"minHit\":1 }}" +
        "}";
    //assertEquals(broker.browse(new SenseiRequest()).getNumHits(), 15000);
    JSONObject res = search(new JSONObject(req));
   // System.out.println(res.toString(1));
    assertEquals("numhits is wrong", 3, res.getInt("numhits"));
  }
  public void test1bMultiRangeHandler() throws Exception
  {
   
    logger.info("executing test case Selection terms");
    String req = "{\"selections\":[{\"terms\":{\"groupid_range_multi\":{\"values\":[\"[1 TO 500]\"],\"operator\":\"or\"}}}]" +
        ",\"facets\":{    \"groupid_range_multi\":{\"minHit\":1, \"max\":100, \"properties\":{\"maxFacetsPerKey\":1} }}" + 
        "}";
    //assertEquals(broker.browse(new SenseiRequest()).getNumHits(), 15000);
    JSONObject res = search(new JSONObject(req));
    
    //System.out.println(res.toString(1));
    assertEquals("numhits is wrong",1, res.getInt("numhits"));
  }
  
  public void test1AttributesFacetHandlerTwoOrTerms() throws Exception
  {
   
    logger.info("executing test case Selection terms");
    String req = "{\"selections\":[{\"terms\":{\"object_properties\":{\"values\":[\"key1\", \"key2\"],\"operator\":\"and\"}}}]" +
    		//",\"facets\":{    \"object_properties\":{\"minHit\":1 }}" +
    		"}";
    //assertEquals(broker.browse(new SenseiRequest()).getNumHits(), 15000);
    JSONObject res = search(new JSONObject(req));
    assertEquals("numhits is wrong", 3634, res.getInt("numhits"));
  }
  public void test2AttributesFacetHandlerTwoAndTerms() throws Exception
  {
   
    logger.info("executing test case Selection terms");
    String req = "{\"selections\":[{\"terms\":{\"object_properties\":{\"values\":[\"key1\", \"key2\"],\"operator\":\"or\"}}}]" +
        //",\"facets\":{    \"object_properties\":{\"minHit\":1 }}" +
        "}";
    //assertEquals(broker.browse(new SenseiRequest()).getNumHits(), 15000);
    JSONObject res = search(new JSONObject(req));
    assertEquals("numhits is wrong", 12503, res.getInt("numhits"));
  }
  public void test3AttributesFacetHandlerTwoAndTermsAndFacets() throws Exception
  {
    
    logger.info("executing test case Selection terms");
    String req = "{\"selections\":[{\"terms\":{\"object_properties\":{\"values\":[\"key1\"],\"operator\":\"or\"," +
        "}}}]" +
        ",\"facets\":{    \"object_properties\":{\"minHit\":1, \"max\":100, \"properties\":{\"maxFacetsPerKey\":1} }}" +
        "}";
    //assertEquals(broker.browse(new SenseiRequest()).getNumHits(), 15000);
    JSONObject res = search(new JSONObject(req));
    assertEquals("numhits is wrong", 8180, res.getInt("numhits"));
    assertTrue(res.optJSONObject("facets").optJSONArray("object_properties").length() < 10);
    req = "{\"selections\":[{\"terms\":{\"object_properties\":{\"values\":[\"key1\"],\"operator\":\"or\"," +
        "}}}]" +
        ",\"facets\":{    \"object_properties\":{\"minHit\":1, \"max\":100, \"properties\":{\"maxFacetsPerKey\":10} }}" +
        "}";
    //assertEquals(broker.browse(new SenseiRequest()).getNumHits(), 15000);
    res = search(new JSONObject(req));
    System.out.println(res.toString(1));
    assertTrue(res.optJSONObject("facets").optJSONArray("object_properties").length() > 10);
  }
  public void test4TotalCountWithFacetSpecLimitMaxFacetsPerKey() throws Exception
  {
    logger.info("executing test case testTotalCountWithFacetSpec");
    SenseiRequest req = new SenseiRequest();
    
    FacetSpec facetSpec = new FacetSpec();
    facetSpec.setMaxCount(20);
    facetSpec.setOrderBy(FacetSortSpec.OrderHitsDesc);
    facetSpec.getProperties().put(AttributesFacetHandler.MAX_FACETS_PER_KEY_PROP_NAME, "1");
    setspec(req, facetSpec);
    req.setCount(5);
    req.setFacetSpec("object_properties", facetSpec);
    //setspec(req, facetSpecall);
    SenseiResult res = broker.browse(req);
    List<BrowseFacet> facets = res.getFacetAccessor("object_properties").getFacets();
    assertTrue(facets.toString(),facets.size() > 8);
    assertTrue(facets.toString(), facets.size() <= 20);
    
  
  }
  public void test5FacetsAll() throws Exception
  {
    logger.info("executing test case testTotalCountWithFacetSpec");
    SenseiRequest req = new SenseiRequest();
    
    FacetSpec facetSpec = new FacetSpec();
    facetSpec.setMaxCount(500);
    facetSpec.setOrderBy(FacetSortSpec.OrderHitsDesc);
    facetSpec.getProperties().put(AttributesFacetHandler.MAX_FACETS_PER_KEY_PROP_NAME, "11");
    setspec(req, facetSpec);
    req.setCount(200);
    req.setFacetSpec("object_properties", facetSpec);
    //setspec(req, facetSpecall);
    SenseiResult res = broker.browse(req);
    List<BrowseFacet> facets = res.getFacetMap().get("object_properties").getFacets();
    assertEquals("" + facets.size(), 100, facets.size() );  
  }
  
  private JSONObject search(JSONObject req) throws Exception  {
    return  search(SenseiStarter.SenseiUrl, req.toString());
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
