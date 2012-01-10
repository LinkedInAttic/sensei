package com.sensei.test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;

import junit.framework.TestCase;

import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import com.browseengine.bobo.api.FacetSpec;
import com.sensei.search.nodes.SenseiBroker;
import com.sensei.search.req.SenseiRequest;
import com.sensei.search.req.SenseiResult;
import com.sensei.search.svc.api.SenseiService;

public class TestSenseiAttributesHandler extends TestCase {

  private static final Logger logger = Logger.getLogger(TestSenseiAttributesHandler.class);

  private static SenseiBroker broker;
  private static SenseiService httpRestSenseiService;
  static {
    SenseiStarter.start();
    broker = SenseiStarter.broker;
    httpRestSenseiService = SenseiStarter.httpRestSenseiService;

  }

  public void ntestSelectionDynamicTimeRangeJson() throws Exception
  {
    logger.info("executing test case Selection terms");
    String req = "{"+
        "\"facets\":{    \"tags_attributes\":{\"minHit\":1 }}" +
        "}";
    System.out.println(req);
    JSONObject res = search(new JSONObject(req));
    System.out.println(res.toString(1));
    assertEquals("numhits is wrong", 12990, res.getInt("numhits"));
  }
  public void testTotalCountWithFacetSpec() throws Exception
  {
    logger.info("executing test case testTotalCountWithFacetSpec");
    SenseiRequest req = new SenseiRequest();
    
    FacetSpec facetSpec = new FacetSpec();
    facetSpec.setMaxCount(30);
    setspec(req, facetSpec);
    req.setCount(5);
    req.setFacetSpec("tags_attributes", facetSpec);
    //setspec(req, facetSpecall);
    SenseiResult res = broker.browse(req);
    System.out.println("!!!" + res.toString());
  
  }
  public void testAttributesFacetHandlerWithTwoTerms() throws Exception
  {
    logger.info("executing test case Selection terms");
    String req = "{\"selections\":[{\"terms\":{\"tags_attributes\":{\"values\":[\"automatic\",\"cool\"],\"operator\":\"or\"}}}]" +
    		",\"facets\":{    \"tags_attributes\":{\"minHit\":1 }}}";
    JSONObject res = search(new JSONObject(req));
   
   
    System.out.println("!!!" + res.toString(1));
  }
  public void testAttributesFacetHandlerTwoTermsAndOneExcluded() throws Exception
  {
    logger.info("executing test case Selection terms");
    String req = "{\"selections\":[{\"terms\":{\"tags_attributes\":{\"values\":[\"electric\",\"cool\"],\"excludes\":[\"automatic\"],\"operator\":\"or\"}}}]" +
        ",\"facets\":{    \"tags_attributes\":{\"minHit\":1 }}}";
    JSONObject res = search(new JSONObject(req));
   
   
    System.out.println("!!!" + res.toString(1));
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
