package com.senseidb.test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import junit.framework.TestCase;

import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.browseengine.bobo.api.BrowseFacet;
import com.browseengine.bobo.api.BrowseSelection;
import com.browseengine.bobo.api.FacetAccessible;
import com.browseengine.bobo.api.FacetSpec;
import com.browseengine.bobo.api.FacetSpec.FacetSortSpec;
import com.browseengine.bobo.facets.DefaultFacetHandlerInitializerParam;
import com.senseidb.search.node.SenseiBroker;
import com.senseidb.search.req.SenseiHit;
import com.senseidb.search.req.SenseiRequest;
import com.senseidb.search.req.SenseiResult;
import com.senseidb.svc.api.SenseiService;

public class TestSensei extends TestCase {

  private static final Logger logger = Logger.getLogger(TestSensei.class);

  private static SenseiBroker broker;
  private static SenseiService httpRestSenseiService;
  static {
    SenseiStarter.start("test-conf/node1","test-conf/node2");
    broker = SenseiStarter.broker;
    httpRestSenseiService = SenseiStarter.httpRestSenseiService;

  }

  public void testTotalCount() throws Exception
  {
    logger.info("executing test case testTotalCount");
    SenseiRequest req = new SenseiRequest();
    SenseiResult res = broker.browse(req);
    assertEquals("wrong total number of hits" + req + res, 15000, res.getNumHits());
    logger.info("request:" + req + "\nresult:" + res);
  }

  public void testTotalCountWithFacetSpec() throws Exception
  {
    logger.info("executing test case testTotalCountWithFacetSpec");
    SenseiRequest req = new SenseiRequest();
    FacetSpec facetSpecall = new FacetSpec();
    facetSpecall.setMaxCount(1000000);
    facetSpecall.setExpandSelection(true);
    facetSpecall.setMinHitCount(0);
    facetSpecall.setOrderBy(FacetSortSpec.OrderHitsDesc);
    FacetSpec facetSpec = new FacetSpec();
    facetSpec.setMaxCount(5);
    setspec(req, facetSpec);
    req.setCount(5);
    setspec(req, facetSpecall);
    SenseiResult res = broker.browse(req);
    logger.info("request:" + req + "\nresult:" + res);
    verifyFacetCount(res, "year", "[1993 TO 1994]", 3090);
  }

  public void testSelection() throws Exception
  {
    logger.info("executing test case testSelection");
    FacetSpec facetSpecall = new FacetSpec();
    facetSpecall.setMaxCount(1000000);
    facetSpecall.setExpandSelection(true);
    facetSpecall.setMinHitCount(0);
    facetSpecall.setOrderBy(FacetSortSpec.OrderHitsDesc);
    FacetSpec facetSpec = new FacetSpec();
    facetSpec.setMaxCount(5);
    SenseiRequest req = new SenseiRequest();
    req.setCount(3);
    facetSpecall.setMaxCount(3);
    setspec(req, facetSpecall);
    BrowseSelection sel = new BrowseSelection("year");
    String selVal = "[2001 TO 2002]";
    sel.addValue(selVal);
    req.addSelection(sel);
    SenseiResult res = broker.browse(req);
    logger.info("request:" + req + "\nresult:" + res);
    assertEquals(2907, res.getNumHits());
    String selName = "year";
    verifyFacetCount(res, selName, selVal, 2907);
    verifyFacetCount(res, "year", "[1993 TO 1994]", 3090);
  }
 
  public void testSelectionNot() throws Exception
  {
    logger.info("executing test case testSelectionNot");
    FacetSpec facetSpecall = new FacetSpec();
    facetSpecall.setMaxCount(1000000);
    facetSpecall.setExpandSelection(true);
    facetSpecall.setMinHitCount(0);
    facetSpecall.setOrderBy(FacetSortSpec.OrderHitsDesc);
    FacetSpec facetSpec = new FacetSpec();
    facetSpec.setMaxCount(5);
    SenseiRequest req = new SenseiRequest();
    req.setCount(3);
    facetSpecall.setMaxCount(3);
    setspec(req, facetSpecall);
    BrowseSelection sel = new BrowseSelection("year");
    sel.addNotValue("[2001 TO 2002]");
    req.addSelection(sel);
    SenseiResult res = broker.browse(req);
    logger.info("request:" + req + "\nresult:" + res);
    assertEquals(12093, res.getNumHits());
    verifyFacetCount(res, "year", "[1993 TO 1994]", 3090);
  }

  public void testGroupBy() throws Exception
  {
    logger.info("executing test case testGroupBy");
    SenseiRequest req = new SenseiRequest();
    req.setCount(1);
    req.setGroupBy(new String[]{"groupid"});
    SenseiResult res = broker.browse(req);
    logger.info("request:" + req + "\nresult:" + res);
    SenseiHit hit = res.getSenseiHits()[0];
    assertTrue(hit.getGroupHitsCount() > 0);
  }

  public void testGroupByWithGroupedHits() throws Exception
  {
    logger.info("executing test case testGroupBy");
    SenseiRequest req = new SenseiRequest();
    req.setCount(1);
    req.setGroupBy(new String[]{"groupid"});
    req.setMaxPerGroup(8);
    SenseiResult res = broker.browse(req);
    logger.info("request:" + req + "\nresult:" + res);
    SenseiHit hit = res.getSenseiHits()[0];
    assertTrue(hit.getGroupHitsCount() > 0);
    assertTrue(hit.getSenseiGroupHits().length > 0);

    // use httpRestSenseiService
    res = httpRestSenseiService.doQuery(req);
    logger.info("request:" + req + "\nresult:" + res);
    hit = res.getSenseiHits()[0];
    assertTrue(hit.getGroupHitsCount() > 0);
    assertTrue(hit.getSenseiGroupHits().length > 0);
  }

  public void testGroupByVirtual() throws Exception
  {
    logger.info("executing test case testGroupByVirtual");
    SenseiRequest req = new SenseiRequest();
    req.setCount(1);
    req.setGroupBy(new String[]{"virtual_groupid"});
    SenseiResult res = broker.browse(req);
    logger.info("request:" + req + "\nresult:" + res);
    SenseiHit hit = res.getSenseiHits()[0];
    assertTrue(hit.getGroupHitsCount() > 0);
  }

  public void testGroupByVirtualWithGroupedHits() throws Exception
  {
    logger.info("executing test case testGroupByVirtualWithGroupedHits");
    SenseiRequest req = new SenseiRequest();
    req.setCount(1);
    req.setGroupBy(new String[]{"virtual_groupid"});
    req.setMaxPerGroup(8);
    SenseiResult res = broker.browse(req);
    logger.info("request:" + req + "\nresult:" + res);
    SenseiHit hit = res.getSenseiHits()[0];
    assertTrue(hit.getGroupHitsCount() > 0);
    assertTrue(hit.getSenseiGroupHits().length > 0);
  }

  public void testGroupByFixedLengthLongArray() throws Exception
  {
    logger.info("executing test case testGroupByFixedLengthLongArray");
    SenseiRequest req = new SenseiRequest();
    req.setCount(1);
    req.setGroupBy(new String[]{"virtual_groupid_fixedlengthlongarray"});
    SenseiResult res = broker.browse(req);
    logger.info("request:" + req + "\nresult:" + res);
    SenseiHit hit = res.getSenseiHits()[0];
    assertTrue(hit.getGroupHitsCount() > 0);
  }

  public void testGroupByFixedLengthLongArrayWithGroupedHits() throws Exception
  {
    logger.info("executing test case testGroupByFixedLengthLongArrayWithGroupedHits");
    SenseiRequest req = new SenseiRequest();
    req.setCount(1);
    req.setGroupBy(new String[]{"virtual_groupid_fixedlengthlongarray"});
    req.setMaxPerGroup(8);
    SenseiResult res = broker.browse(req);
    logger.info("request:" + req + "\nresult:" + res);
    SenseiHit hit = res.getSenseiHits()[0];
    assertTrue(hit.getGroupHitsCount() > 0);
    assertTrue(hit.getSenseiGroupHits().length > 0);
  }

  public void testBQL1() throws Exception
  {
    logger.info("Executing test case testBQL1");
    String req = "{\"bql\":\"select * from cars\"}";
    JSONObject res = search(new JSONObject(req));
    assertEquals("numhits is wrong", 15000, res.getInt("numhits"));
  }

  public void testBQL2() throws Exception
  {
    logger.info("Executing test case testBQL2");
    String req = "{\"bql\":\"select * from cars where color = 'red'\"}";
    JSONObject res = search(new JSONObject(req));
    assertEquals("numhits is wrong", 2160, res.getInt("numhits"));
  }

  public void testBqlExtraFilter() throws Exception
  {
    logger.info("Executing test case testBqlExtraFilter");
    String req = "{\"bql\":\"select * from cars where color = 'red'\", \"bql_extra_filter\":\"year < 2000\"}";
    JSONObject res = search(new JSONObject(req));
    assertEquals("numhits is wrong", 1534, res.getInt("numhits"));
  }
  
  public void testBqlEmptyListCheck() throws Exception
  {
    logger.info("Executing test case testBqlEmptyListCheck");
    String req = "{\"bql\":\"SELECT * FROM SENSEI where () is not empty LIMIT 0, 1\"}";
    JSONObject res = search(new JSONObject(req));
    assertEquals("numhits is wrong", 0, res.getInt("numhits"));
    
    String req2 = "{\"bql\":\"SELECT * FROM SENSEI where () is empty LIMIT 0, 1\"}";
    JSONObject res2 = search(new JSONObject(req2));
    assertEquals("numhits is wrong", 15000, res2.getInt("numhits"));
    
    String req3 = "{\"bql\":\"select * from sensei where () is empty or color contains all () limit 0, 1\"}";
    JSONObject res3 = search(new JSONObject(req3));
    assertEquals("numhits is wrong", 15000, res3.getInt("numhits"));
    
    String req4 = "{\"bql\":\"select * from sensei where () is not empty or color contains all () limit 0, 1\"}";
    JSONObject res4 = search(new JSONObject(req4));
    assertEquals("numhits is wrong", 0, res4.getInt("numhits"));
    
    //template mapping:
    String req5 = "{\"bql\":\"SELECT * FROM SENSEI where $list is empty LIMIT 0, 1\", \"templateMapping\":{\"list\":[\"a\"]}}";
    JSONObject res5 = search(new JSONObject(req5));
    assertEquals("numhits is wrong", 0, res5.getInt("numhits"));
    
    String req6 = "{\"bql\":\"SELECT * FROM SENSEI where $list is empty LIMIT 0, 1\", \"templateMapping\":{\"list\":[]}}";
    JSONObject res6 = search(new JSONObject(req6));
    assertEquals("numhits is wrong", 15000, res6.getInt("numhits"));
  }

  public void testBqlRelevance1() throws Exception
  {
    logger.info("Executing test case testBqlRelevance1");
    String req = "{\"bql\":\"SELECT * FROM cars USING RELEVANCE MODEL my_model (thisYear:2001, goodYear:[1996]) DEFINED AS (int thisYear, IntOpenHashSet goodYear) BEGIN if (goodYear.contains(year)) return (float)Math.exp(10d); if (year==thisYear) return 87f; return _INNER_SCORE; END\"}";
    
    JSONObject res = search(new JSONObject(req));
    System.out.println("!!!rel" + res.toString(1));
    JSONArray hits = res.getJSONArray("hits");
    JSONObject firstHit = hits.getJSONObject(0);
    JSONObject secondHit = hits.getJSONObject(1);
    
    String firstYear = firstHit.getJSONArray("year").getString(0);
    String secondYear = secondHit.getJSONArray("year").getString(0);
    
    assertEquals("year 1996 should be on the top", true, firstYear.contains("1996"));
    assertEquals("year 1996 should be on the top", true, secondYear.contains("1996"));
  }

  public void testSelectionTerm() throws Exception
  {
    logger.info("executing test case Selection term");
    String req = "{\"selections\":[{\"term\":{\"color\":{\"value\":\"red\"}}}]}";
    JSONObject res = search(new JSONObject(req));
    assertEquals("numhits is wrong", 2160, res.getInt("numhits"));
  }

  public void testSelectionTerms() throws Exception
  {
    logger.info("executing test case Selection terms");
    String req = "{\"selections\":[{\"terms\":{\"tags\":{\"values\":[\"mp3\",\"moon-roof\"],\"excludes\":[\"leather\"],\"operator\":\"or\"}}}]}";
    JSONObject res = search(new JSONObject(req));
    assertEquals("numhits is wrong", 4483, res.getInt("numhits"));
  }

  public void testSelectionDynamicTimeRangeJson() throws Exception
  {
    logger.info("executing test case Selection terms");
    String req = "{\"selections\":[{\"term\":{\"timeRange\":{\"value\":\"000000013\"}}}]" +
    		", \"facetInit\":{    \"timeRange\":{\"time\" :{  \"type\" : \"long\",\"values\" : [15000] }}}" +
    		"}";
    System.out.println(req);
    JSONObject res = search(new JSONObject(req));
    assertEquals("numhits is wrong", 12990, res.getInt("numhits"));
  }
 
  public void testSelectionDynamicTimeRangeJson2() throws Exception
  {
    // Test scalar values in facet init parameters
    logger.info("executing test case Selection terms");
    String req = "{\"selections\":[{\"term\":{\"timeRange\":{\"value\":\"000000013\"}}}]" +
    		", \"facetInit\":{    \"timeRange\":{\"time\" :{  \"type\" : \"long\",\"values\" : 15000 }}}" +
    		"}";
    System.out.println(req);
    JSONObject res = search(new JSONObject(req));
    assertEquals("numhits is wrong", 12990, res.getInt("numhits"));
  }
  public void testSelectionRange2() throws Exception
  {
    //2000 1548;
    //2001 1443;
    //2002 1464;
    // [2000 TO 2002]   ==> 4455
    // (2000 TO 2002)   ==> 1443
    // (2000 TO 2002]   ==> 2907
    // [2000 TO 2002)   ==> 2991
    {
      logger.info("executing test case Selection range [2000 TO 2002]");
      String req = "{\"selections\":[{\"range\":{\"year\":{\"to\":\"2002\",\"include_lower\":true,\"include_upper\":true,\"from\":\"2000\"}}}]}";
      JSONObject res = search(new JSONObject(req));
      assertEquals("numhits is wrong", 4455, res.getInt("numhits"));
    }
  }
  public void testSelectionRange() throws Exception
  {
    //2000 1548;
    //2001 1443;
    //2002 1464;
    // [2000 TO 2002]   ==> 4455
    // (2000 TO 2002)   ==> 1443
    // (2000 TO 2002]   ==> 2907
    // [2000 TO 2002)   ==> 2991
    {
      logger.info("executing test case Selection range [2000 TO 2002]");
      String req = "{\"selections\":[{\"range\":{\"year\":{\"to\":\"2002\",\"include_lower\":true,\"include_upper\":true,\"from\":\"2000\"}}}]}";
      JSONObject res = search(new JSONObject(req));
      assertEquals("numhits is wrong", 4455, res.getInt("numhits"));
    }

    {
      logger.info("executing test case Selection range (2000 TO 2002)");
      String req = "{\"selections\":[{\"range\":{\"year\":{\"to\":\"2002\",\"include_lower\":false,\"include_upper\":false,\"from\":\"2000\"}}}]}";
      JSONObject res = search(new JSONObject(req));
      assertEquals("numhits is wrong", 1443, res.getInt("numhits"));
    }

    {
      logger.info("executing test case Selection range (2000 TO 2002]");
      String req = "{\"selections\":[{\"range\":{\"year\":{\"to\":\"2002\",\"include_lower\":false,\"include_upper\":true,\"from\":\"2000\"}}}]}";
      JSONObject res = search(new JSONObject(req));
      assertEquals("numhits is wrong", 2907, res.getInt("numhits"));
    }

    {
      logger.info("executing test case Selection range [2000 TO 2002)");
      String req = "{\"selections\":[{\"range\":{\"year\":{\"to\":\"2002\",\"include_lower\":true,\"include_upper\":false,\"from\":\"2000\"}}}]}";
      JSONObject res = search(new JSONObject(req));
      assertEquals("numhits is wrong", 2991, res.getInt("numhits"));
    }

  }

  public void testMatchAllWithBoostQuery() throws Exception
  {
    logger.info("executing test case MatchAllQuery");
    String req = "{\"query\": {\"match_all\": {\"boost\": \"1.2\"}}}";
    JSONObject res = search(new JSONObject(req));
    assertEquals("numhits is wrong", 15000, res.getInt("numhits"));
  }

  public void testQueryStringQuery() throws Exception
  {
    logger.info("executing test case testQueryStringQuery");
    String req = "{\"query\": {\"query_string\": {\"query\": \"red AND cool\"}}}";
    JSONObject res = search(new JSONObject(req));
    assertEquals("numhits is wrong", 1070, res.getInt("numhits"));
  }

  public void testMatchAllQuery() throws Exception
  {
    logger.info("executing test case testMatchAllQuery");
    String req = "{\"query\": {\"match_all\": {}}}";
    JSONObject res = search(new JSONObject(req));
    assertEquals("numhits is wrong", 15000, res.getInt("numhits"));
  }

  public void testUIDQuery() throws Exception
  {
    logger.info("executing test case testUIDQuery");
    String req = "{\"query\": {\"ids\": {\"values\": [\"1\", \"4\", \"3\", \"2\", \"6\"], \"excludes\": [\"2\"]}}}";
    JSONObject res = search(new JSONObject(req));

    assertEquals("numhits is wrong", 4, res.getInt("numhits"));
    Set<Integer> expectedIds = new HashSet(Arrays.asList(new Integer[]{1, 3, 4, 6}));
    for (int i = 0; i < res.getInt("numhits"); ++i)
    {
      int uid = res.getJSONArray("hits").getJSONObject(i).getInt("_uid");
      assertTrue("_UID " + uid + " is not expected.", expectedIds.contains(uid));
    }
  }

  public void testTextQuery() throws Exception
  {
    logger.info("executing test case testTextQuery");
    String req = "{\"query\": {\"text\": {\"contents\": { \"value\": \"red cool\", \"operator\": \"and\"}}}}";
    JSONObject res = search(new JSONObject(req));
    assertEquals("numhits is wrong", 1070, res.getInt("numhits"));
  }

  public void testTermQuery() throws Exception
  {
    logger.info("executing test case testTermQuery");
    String req = "{\"query\":{\"term\":{\"color\":\"red\"}}}";
    JSONObject res = search(new JSONObject(req));
    assertEquals("numhits is wrong", 2160, res.getInt("numhits"));
  }

  public void testTermsQuery() throws Exception
  {
    logger.info("executing test case testTermQuery");
    String req = "{\"query\":{\"terms\":{\"tags\":{\"values\":[\"leather\",\"moon-roof\"],\"excludes\":[\"hybrid\"],\"minimum_match\":0,\"operator\":\"or\"}}}}";
    JSONObject res = search(new JSONObject(req));
    assertEquals("numhits is wrong", 5777, res.getInt("numhits"));
  }


  public void testBooleanQuery() throws Exception
  {
    logger.info("executing test case testBooleanQuery");
    String req = "{\"query\":{\"bool\":{\"must_not\":{\"term\":{\"category\":\"compact\"}},\"must\":{\"term\":{\"color\":\"red\"}}}}}";
    JSONObject res = search(new JSONObject(req));
    assertEquals("numhits is wrong", 1652, res.getInt("numhits"));
  }


  public void testDistMaxQuery() throws Exception
  {
    //color red ==> 2160
    //color blue ==> 1104
    logger.info("executing test case testDistMaxQuery");
    String req = "{\"query\":{\"dis_max\":{\"tie_breaker\":0.7,\"queries\":[{\"term\":{\"color\":\"red\"}},{\"term\":{\"color\":\"blue\"}}],\"boost\":1.2}}}";
    JSONObject res = search(new JSONObject(req));
    assertEquals("numhits is wrong", 3264, res.getInt("numhits"));
  }

  public void testPathQuery() throws Exception
  {
   
    logger.info("executing test case testPathQuery");
    String req = "{\"query\":{\"path\":{\"makemodel\":\"asian/acura/3.2tl\"}}}";
    JSONObject res = search(new JSONObject(req));
    assertEquals("numhits is wrong", 126, res.getInt("numhits"));
  }

  public void testPrefixQuery() throws Exception
  {
    //color blue ==> 1104
    logger.info("executing test case testPrefixQuery");
    String req = "{\"query\":{\"prefix\":{\"color\":{\"value\":\"blu\",\"boost\":2}}}}";
    JSONObject res = search(new JSONObject(req));
    assertEquals("numhits is wrong", 1104, res.getInt("numhits"));
  }


  public void testWildcardQuery() throws Exception
  {
    //color blue ==> 1104
    logger.info("executing test case testWildcardQuery");
    String req = "{\"query\":{\"wildcard\":{\"color\":{\"value\":\"bl*e\",\"boost\":2}}}}";
    JSONObject res = search(new JSONObject(req));
    assertEquals("numhits is wrong", 1104, res.getInt("numhits"));
  }

  public void testRangeQuery() throws Exception
  {   
    logger.info("executing test case testRangeQuery");
    String req = "{\"query\":{\"range\":{\"year\":{\"to\":2000,\"boost\":2,\"from\":1999,\"_noOptimize\":false}}}}";
    JSONObject res = search(new JSONObject(req));
    assertEquals("numhits is wrong", 3015, res.getInt("numhits"));
  }

  public void testRangeQuery2() throws Exception
  {
    logger.info("executing test case testRangeQuery2");
    String req = "{\"query\":{\"range\":{\"year\":{\"to\":\"2000\",\"boost\":2,\"from\":\"1999\",\"_noOptimize\":true,\"_type\":\"int\"}}}}";
    JSONObject res = search(new JSONObject(req));
    assertEquals("numhits is wrong", 3015, res.getInt("numhits"));
  }


  public void testFilteredQuery() throws Exception
  {
    logger.info("executing test case testFilteredQuery");
    String req ="{\"query\":{\"filtered\":{\"query\":{\"term\":{\"color\":\"red\"}},\"filter\":{\"range\":{\"year\":{\"to\":2000,\"from\":1999}}}}}}";
    JSONObject res = search(new JSONObject(req));
    assertEquals("numhits is wrong", 447, res.getInt("numhits"));
  }


  public void testSpanTermQuery() throws Exception
  {
    logger.info("executing test case testSpanTermQuery");
    String req = "{\"query\":{\"span_term\":{\"color\":\"red\"}}}";
    JSONObject res = search(new JSONObject(req));
    assertEquals("numhits is wrong", 2160, res.getInt("numhits"));
  }


  public void testSpanOrQuery() throws Exception
  {
    logger.info("executing test case testSpanOrQuery");
    String req = "{\"query\":{\"span_or\":{\"clauses\":[{\"span_term\":{\"color\":\"red\"}},{\"span_term\":{\"color\":\"blue\"}}]}}}";
    JSONObject res = search(new JSONObject(req));
    assertEquals("numhits is wrong", 3264, res.getInt("numhits"));
  }


  public void testSpanNotQuery() throws Exception
  {
    logger.info("executing test case testSpanNotQuery");
    String req = "{\"query\":{\"span_not\":{\"exclude\":{\"span_term\":{\"contents\":\"red\"}},\"include\":{\"span_term\":{\"contents\":\"compact\"}}}}}";
    JSONObject res = search(new JSONObject(req));
    assertEquals("numhits is wrong", 4596, res.getInt("numhits"));
  }

  public void testSpanNearQuery1() throws Exception
  {
    logger.info("executing test case testSpanNearQuery1");
    String req = "{\"query\":{\"span_near\":{\"in_order\":false,\"collect_payloads\":false,\"slop\":12,\"clauses\":[{\"span_term\":{\"contents\":\"red\"}},{\"span_term\":{\"contents\":\"compact\"}},{\"span_term\":{\"contents\":\"hybrid\"}}]}}}";
    JSONObject res = search(new JSONObject(req));
    assertEquals("numhits is wrong", 274, res.getInt("numhits"));
  }

  public void testSpanNearQuery2() throws Exception
  {
    logger.info("executing test case testSpanNearQuery2");
    String req = "{\"query\":{\"span_near\":{\"in_order\":true,\"collect_payloads\":false,\"slop\":0,\"clauses\":[{\"span_term\":{\"contents\":\"red\"}},{\"span_term\":{\"contents\":\"compact\"}},{\"span_term\":{\"contents\":\"favorite\"}}]}}}";
    JSONObject res = search(new JSONObject(req));
    assertEquals("numhits is wrong", 63, res.getInt("numhits"));
  }

  public void testSpanFirstQuery() throws Exception
  {
    logger.info("executing test case testSpanFirstQuery");
    String req = "{\"query\":{\"span_first\":{\"match\":{\"span_term\":{\"color\":\"red\"}},\"end\":2}}}";
    JSONObject res = search(new JSONObject(req));
    assertEquals("numhits is wrong", 2160, res.getInt("numhits"));
  }
  
  public void testConstExpQuery() throws Exception
  {
    logger.info("executing test case testConstExpQuery");

    String pos_req1 = "{\"query\":{\"const_exp\":{\"lvalue\":\"6\",\"rvalue\":\"6\",\"operator\":\"==\"}},\"fetchStored\":false,\"from\":0,\"explain\":false,\"size\":10}";
    String pos_req2 = "{\"query\":{\"const_exp\":{\"lvalue\":6,\"rvalue\":6,\"operator\":\"==\"}},\"fetchStored\":false,\"from\":0,\"explain\":false,\"size\":10}";
    String pos_req3 = "{\"query\":{\"const_exp\":{\"lvalue\":[6,7],\"rvalue\":[6,7],\"operator\":\"==\"}},\"fetchStored\":false,\"from\":0,\"explain\":false,\"size\":10}";
    String pos_req4 = "{\"query\":{\"const_exp\":{\"lvalue\":[7],\"rvalue\":[7],\"operator\":\"==\"}},\"fetchStored\":false,\"from\":0,\"explain\":false,\"size\":10}";
    String pos_req5 = "{\"query\":{\"const_exp\":{\"lvalue\":[\"7\",\"8\"],\"rvalue\":[\"8\",\"7\"],\"operator\":\"==\"}},\"fetchStored\":false,\"from\":0,\"explain\":false,\"size\":10}";
    String pos_req6 = "{\"query\":{\"const_exp\":{\"lvalue\":6,\"rvalue\":[6,7],\"operator\":\"in\"}},\"fetchStored\":false,\"from\":0,\"explain\":false,\"size\":10}";
    String pos_req7 = "{\"query\":{\"const_exp\":{\"lvalue\":[6],\"rvalue\":[6,7],\"operator\":\"in\"}},\"fetchStored\":false,\"from\":0,\"explain\":false,\"size\":10}";
    String pos_req8 = "{\"query\":{\"const_exp\":{\"lvalue\":6,\"rvalue\":[16,7],\"operator\":\"not_in\"}},\"fetchStored\":false,\"from\":0,\"explain\":false,\"size\":10}";
    String pos_req9 = "{\"query\":{\"const_exp\":{\"lvalue\":6,\"rvalue\":3,\"operator\":\">\"}},\"fetchStored\":false,\"from\":0,\"explain\":false,\"size\":10}";
    String pos_req10 = "{\"query\":{\"const_exp\":{\"lvalue\":6,\"rvalue\":6,\"operator\":\">=\"}},\"fetchStored\":false,\"from\":0,\"explain\":false,\"size\":10}";

    String pos_req11 = "{\"query\":{\"const_exp\":{\"lvalue\":{\"params\":[[1]],\"function\":\"length\"},\"rvalue\":0,\"operator\":\">=\"}},\"fetchStored\":false,\"from\":0,\"explain\":false,\"size\":10}";
    String pos_req12 = "{\"query\":{\"const_exp\":{\"lvalue\":{\"params\":[[1,2]],\"function\":\"length\"},\"rvalue\":0,\"operator\":\">=\"}},\"fetchStored\":false,\"from\":0,\"explain\":false,\"size\":10}";
    String pos_req13 = "{\"query\":{\"const_exp\":{\"lvalue\":{\"params\":[[\"1\"]],\"function\":\"length\"},\"rvalue\":0,\"operator\":\">=\"}},\"fetchStored\":false,\"from\":0,\"explain\":false,\"size\":10}";
    String pos_req14 = "{\"query\":{\"const_exp\":{\"lvalue\":{\"params\":[[\"1\",\"2\"]],\"function\":\"length\"},\"rvalue\":0,\"operator\":\">=\"}},\"fetchStored\":false,\"from\":0,\"explain\":false,\"size\":10}";
    String pos_req15 = "{\"query\":{\"const_exp\":{\"lvalue\":{\"params\":[[]],\"function\":\"length\"},\"rvalue\":0,\"operator\":\"==\"}},\"fetchStored\":false,\"from\":0,\"explain\":false,\"size\":10}";
    
    assertEquals("numhits is wrong pos_req1", 15000, search(new JSONObject(pos_req1)).getInt("numhits"));
    assertEquals("numhits is wrong pos_req2", 15000, search(new JSONObject(pos_req2)).getInt("numhits"));
    assertEquals("numhits is wrong pos_req3", 15000, search(new JSONObject(pos_req3)).getInt("numhits"));
    assertEquals("numhits is wrong pos_req4", 15000, search(new JSONObject(pos_req4)).getInt("numhits"));
    assertEquals("numhits is wrong pos_req5", 15000, search(new JSONObject(pos_req5)).getInt("numhits"));
    assertEquals("numhits is wrong pos_req6", 15000, search(new JSONObject(pos_req6)).getInt("numhits"));
    assertEquals("numhits is wrong pos_req7", 15000, search(new JSONObject(pos_req7)).getInt("numhits"));
    assertEquals("numhits is wrong pos_req8", 15000, search(new JSONObject(pos_req8)).getInt("numhits"));
    assertEquals("numhits is wrong pos_req9", 15000, search(new JSONObject(pos_req9)).getInt("numhits"));
    assertEquals("numhits is wrong pos_req10", 15000, search(new JSONObject(pos_req10)).getInt("numhits"));
    assertEquals("numhits is wrong pos_req11", 15000, search(new JSONObject(pos_req11)).getInt("numhits"));
    assertEquals("numhits is wrong pos_req12", 15000, search(new JSONObject(pos_req12)).getInt("numhits"));
    assertEquals("numhits is wrong pos_req13", 15000, search(new JSONObject(pos_req13)).getInt("numhits"));
    assertEquals("numhits is wrong pos_req14", 15000, search(new JSONObject(pos_req14)).getInt("numhits"));
    assertEquals("numhits is wrong pos_req15", 15000, search(new JSONObject(pos_req15)).getInt("numhits"));
    
    
    String neg_req1 = "{\"query\":{\"const_exp\":{\"lvalue\":6,\"rvalue\":[16,7],\"operator\":\"in\"}},\"fetchStored\":false,\"from\":0,\"explain\":false,\"size\":10}";
    String neg_req2 = "{\"query\":{\"const_exp\":{\"lvalue\":[6],\"rvalue\":[5,7],\"operator\":\"in\"}},\"fetchStored\":false,\"from\":0,\"explain\":false,\"size\":10}";
    String neg_req3 = "{\"query\":{\"const_exp\":{\"lvalue\":6,\"rvalue\":[6,7],\"operator\":\"not_in\"}},\"fetchStored\":false,\"from\":0,\"explain\":false,\"size\":10}";
    String neg_req4 = "{\"query\":{\"const_exp\":{\"lvalue\":[6],\"rvalue\":[6,7],\"operator\":\"not_in\"}},\"fetchStored\":false,\"from\":0,\"explain\":false,\"size\":10}";
    String neg_req5 = "{\"query\":{\"const_exp\":{\"lvalue\":6,\"rvalue\":8,\"operator\":\">\"}},\"fetchStored\":false,\"from\":0,\"explain\":false,\"size\":10}";

    String neg_req6 = "{\"query\":{\"const_exp\":{\"lvalue\":{\"params\":[[]],\"function\":\"length\"},\"rvalue\":0,\"operator\":\">\"}},\"fetchStored\":false,\"from\":0,\"explain\":false,\"size\":10}";
    String neg_req7 = "{\"query\":{\"const_exp\":{\"lvalue\":{\"params\":[[4,5]],\"function\":\"length\"},\"rvalue\":0,\"operator\":\"==\"}},\"fetchStored\":false,\"from\":0,\"explain\":false,\"size\":10}";

    assertEquals("numhits is wrong neg_req1", 0, search(new JSONObject(neg_req1)).getInt("numhits"));
    assertEquals("numhits is wrong neg_req2", 0, search(new JSONObject(neg_req2)).getInt("numhits"));
    assertEquals("numhits is wrong neg_req3", 0, search(new JSONObject(neg_req3)).getInt("numhits"));
    assertEquals("numhits is wrong neg_req4", 0, search(new JSONObject(neg_req4)).getInt("numhits"));
    assertEquals("numhits is wrong neg_req5", 0, search(new JSONObject(neg_req5)).getInt("numhits"));
    assertEquals("numhits is wrong neg_req6", 0, search(new JSONObject(neg_req6)).getInt("numhits"));
    assertEquals("numhits is wrong neg_req7", 0, search(new JSONObject(neg_req7)).getInt("numhits"));   
    
  }
  
  public void testNullMultiFilter() throws Exception
  {
    logger.info("executing test case testNullFilter");
    String req = "{\"filter\":{\"isNull\":\"groupid_multi\"}}";
    JSONObject res = search(new JSONObject(req));
    assertEquals("numhits is wrong", 14997, res.getInt("numhits"));
  }
  
  public void testNullFilterOnSimpleColumn() throws Exception
  {
    logger.info("executing test case testNullFilter");
    String req = "{\"filter\":{\"isNull\":\"price\"}}";
    JSONObject res = search(new JSONObject(req));
    assertEquals("numhits is wrong", 1, res.getInt("numhits"));
  }
  public void testUIDFilter() throws Exception
  {
    logger.info("executing test case testUIDFilter");
    String req = "{\"filter\": {\"ids\": {\"values\": [\"1\", \"2\", \"3\"], \"excludes\": [\"2\"]}}}";
    JSONObject res = search(new JSONObject(req));

    assertEquals("numhits is wrong", 2, res.getInt("numhits"));
    Set<Integer> expectedIds = new HashSet(Arrays.asList(new Integer[]{1, 3}));
    for (int i = 0; i < res.getInt("numhits"); ++i)
    {
      int uid = res.getJSONArray("hits").getJSONObject(i).getInt("_uid");
      assertTrue("_UID " + uid + " is not expected.", expectedIds.contains(uid));
    }
  }

  public void testAndFilter() throws Exception
  {
    logger.info("executing test case testAndFilter");
    String req = "{\"filter\":{\"and\":[{\"term\":{\"tags\":\"mp3\",\"_noOptimize\":false}},{\"term\":{\"color\":\"red\",\"_noOptimize\":false}}]}}";
    JSONObject res = search(new JSONObject(req));
    assertEquals("numhits is wrong", 439, res.getInt("numhits"));
  }

  public void testOrFilter() throws Exception
  {
    logger.info("executing test case testOrFilter");
    String req = "{\"filter\":{\"or\":[{\"term\":{\"color\":\"blue\",\"_noOptimize\":true}},{\"term\":{\"color\":\"red\",\"_noOptimize\":true}}]}}";
    JSONObject res = search(new JSONObject(req));
    assertEquals("numhits is wrong", 3264, res.getInt("numhits"));
  }

  public void testOrFilter2() throws Exception
  {
    logger.info("executing test case testOrFilter2");
    String req = "{\"filter\":{\"or\":[{\"term\":{\"color\":\"blue\",\"_noOptimize\":false}},{\"term\":{\"color\":\"red\",\"_noOptimize\":false}}]}}";
    JSONObject res = search(new JSONObject(req));
    assertEquals("numhits is wrong", 3264, res.getInt("numhits"));
  }

  public void testOrFilter3() throws Exception
  {
    logger.info("executing test case testOrFilter3");
    String req = "{\"filter\":{\"or\":[{\"term\":{\"color\":\"blue\",\"_noOptimize\":true}},{\"term\":{\"color\":\"red\",\"_noOptimize\":false}}]}}";
    JSONObject res = search(new JSONObject(req));
    assertEquals("numhits is wrong", 3264, res.getInt("numhits"));
  }


  public void testBooleanFilter() throws Exception
  {
    logger.info("executing test case testBooleanFilter");
    String req = "{\"filter\":{\"bool\":{\"must_not\":{\"term\":{\"category\":\"compact\"}},\"should\":[{\"term\":{\"color\":\"red\"}},{\"term\":{\"color\":\"green\"}}],\"must\":{\"term\":{\"color\":\"red\"}}}}}";
    JSONObject res = search(new JSONObject(req));
    assertEquals("numhits is wrong", 1652, res.getInt("numhits"));
  }

  public void testQueryFilter() throws Exception
  {
    logger.info("executing test case testQueryFilter");
    String req = "{\"filter\": {\"query\":{\"range\":{\"year\":{\"to\":2000,\"boost\":2,\"from\":1999,\"_noOptimize\":false}}}}}";
    JSONObject res = search(new JSONObject(req));
    assertEquals("numhits is wrong", 3015, res.getInt("numhits"));
  }

  /* Need to fix the bug in bobo and kamikazi, for details see the following two test cases:*/

  public void testAndFilter1() throws Exception
  {
    logger.info("executing test case testAndFilter1");
    String req = "{\"filter\":{\"and\":[{\"term\":{\"color\":\"blue\",\"_noOptimize\":false}},{\"query\":{\"term\":{\"category\":\"compact\"}}}]}}";
    JSONObject res = search(new JSONObject(req));
    assertEquals("numhits is wrong", 504, res.getInt("numhits"));
  }

  public void testQueryFilter1() throws Exception
  {
    logger.info("executing test case testQueryFilter1");
    String req = "{\"filter\": {\"query\":{\"term\":{\"category\":\"compact\"}}}}";
    JSONObject res = search(new JSONObject(req));
    assertEquals("numhits is wrong", 4169, res.getInt("numhits"));
  }


  /*  another weird bug may exist somewhere in bobo or kamikazi.*/
  /*  In the following two test cases, when modifying the first one by changing "tags" to "tag", it is supposed that
   *  Only the first test case is not correct, but the second one also throw one NPE, which is weird.
   * */
  public void testAndFilter2() throws Exception
  {
    logger.info("executing test case testAndFilter2");
    String req = "{\"filter\":{\"and\":[{\"term\":{\"tags\":\"mp3\",\"_noOptimize\":false}},{\"query\":{\"term\":{\"color\":\"red\"}}}]}}";
    JSONObject res = search(new JSONObject(req));
    assertEquals("numhits is wrong", 439, res.getInt("numhits"));
  }

  public void testOrFilter4() throws Exception
  {
    //color:blue  ==> 1104
    //color:red   ==> 2160
   logger.info("executing test case testOrFilter4");
    String req = "{\"filter\":{\"or\":[{\"term\":{\"color\":\"blue\",\"_noOptimize\":false}},{\"query\":{\"term\":{\"color\":\"red\"}}}]}}";
    JSONObject res = search(new JSONObject(req));
    assertEquals("numhits is wrong", 3264, res.getInt("numhits"));
  }


  public void testTermFilter() throws Exception
  {
    logger.info("executing test case testTermFilter");
    String req = "{\"filter\":{\"term\":{\"color\":\"red\"}}}";
    JSONObject res = search(new JSONObject(req));
    assertEquals("numhits is wrong", 2160, res.getInt("numhits"));
  }

  public void testTermsFilter() throws Exception
  {
    logger.info("executing test case testTermsFilter");
    String req = "{\"filter\":{\"terms\":{\"tags\":{\"values\":[\"leather\",\"moon-roof\"],\"excludes\":[\"hybrid\"],\"minimum_match\":0,\"operator\":\"or\"}}}}";
    JSONObject res = search(new JSONObject(req));
    assertEquals("numhits is wrong", 5777, res.getInt("numhits"));
  }

  public void testRangeFilter() throws Exception
  {
    logger.info("executing test case testRangeFilter");
    String req = "{\"filter\":{\"range\":{\"year\":{\"to\":2000,\"boost\":2,\"from\":1999,\"_noOptimize\":false}}}}";
    JSONObject res = search(new JSONObject(req));
    assertEquals("numhits is wrong", 3015, res.getInt("numhits"));
  }

  public void testRangeFilter2() throws Exception
  {
    logger.info("executing test case testRangeFilter2");
    String req = "{\"filter\":{\"range\":{\"year\":{\"to\":\"2000\",\"boost\":2,\"from\":\"1999\",\"_noOptimize\":true,\"_type\":\"int\"}}}}";
    JSONObject res = search(new JSONObject(req));
    assertEquals("numhits is wrong", 3015, res.getInt("numhits"));
  }
  public void testRangeFilter3() throws Exception
  {
    logger.info("executing test case testRangeFilter3");
    String req = "{\"fetchStored\":true,\"selections\":[{\"term\":{\"color\":{\"value\":\"red\"}}}],\"from\":0,\"filter\":{\"query\":{\"query_string\":{\"query\":\"cool AND moon-roof AND hybrid\"}}},\"size\":10}";
    JSONObject res = search(new JSONObject(req));
    //TODO Sensei returns undeterministic results for this query. Will create a Jira ticket
    assertTrue("numhits is wrong", res.getInt("numhits") > 10);
  }
  public void testFallbackGroupBy() throws Exception
  {
    logger.info("executing test case testFallbackGroupBy");
    String req = "{\"from\": 0,\"size\": 10,\"groupBy\": {\"columns\": [\"virtual_groupid_fixedlengthlongarray\", \"color\"],\"top\": 2}, \"sort\": [{\"color\": \"asc\"}]}";
    JSONObject res = search(new JSONObject(req));
    JSONArray hits = res.getJSONArray("hits");
    JSONObject firstHit = hits.getJSONObject(0);
    assertTrue("groupfield is wrong", "color".equals(firstHit.getString("groupfield")) || "virtual_groupid_fixedlengthlongarray".equals(firstHit.getString("groupfield")));
    assertTrue("no group hits", firstHit.getJSONArray("grouphits") != null);
  }
  public void testFallbackGroupByWithDistinct() throws Exception
  {
    logger.info("executing test case testFallbackGroupByWithDistinct");
    String req = "{\"bql\": \"SELECT * FROM sensei DISTINCT category GROUP BY virtual_groupid_fixedlengthlongarray OR color TOP 2 ORDER BY color ASC LIMIT 0, 10\"}";
    JSONObject res = search(new JSONObject(req));
    JSONArray hits = res.getJSONArray("hits");
    JSONObject firstHit = hits.getJSONObject(0);
    assertTrue("groupfield is wrong", "color".equals(firstHit.getString("groupfield")) || "virtual_groupid_fixedlengthlongarray".equals(firstHit.getString("groupfield")));
    assertTrue("should be 1 group hit", firstHit.getJSONArray("grouphits").length() == 1);
  }
  public void testGetStoreRequest() throws Exception
  {
    logger.info("executing test case testGetStoreRequest");
    String req = "[1,2,3,5]";
    JSONObject res = searchGet(new JSONArray(req));
    //TODO Sensei returns undeterministic results for this query. Will create a Jira issue
    assertTrue("numhits is wrong", res.length() == 4);
    assertNotNull("", res.get(String.valueOf(1)));
  }

  public void testRelevanceMatchAll() throws Exception
  {
    logger.info("executing test case testRelevanceMatchAll");
    String req = "{\"sort\":[\"_score\"],\"query\":{\"query_string\":{\"query\":\"\",\"relevance\":{\"model\":{\"function_params\":[\"_INNER_SCORE\",\"thisYear\",\"year\",\"goodYear\"],\"facets\":{\"int\":[\"year\",\"mileage\"],\"long\":[\"groupid\"]},\"function\":\" if(goodYear.contains(year)) return (float)Math.exp(10d);   if(year==thisYear) return 87f   ; return  _INNER_SCORE    ;\",\"variables\":{\"set_int\":[\"goodYear\"],\"int\":[\"thisYear\"]}},\"values\":{\"thisYear\":2001,\"goodYear\":[1996,1997]}}}},\"fetchStored\":false,\"from\":0,\"explain\":false,\"size\":6}";
    JSONObject res = search(new JSONObject(req));
    assertEquals("numhits is wrong", 15000, res.getInt("numhits"));
  }
  
  public void testRelevanceHashSet() throws Exception
  {
    logger.info("executing test case testRelevanceHashSet");
    String req = "{\"sort\":[\"_score\"],\"query\":{\"query_string\":{\"query\":\"\",\"relevance\":{\"model\":{\"function_params\":[\"_INNER_SCORE\",\"thisYear\",\"year\",\"goodYear\"],\"facets\":{\"int\":[\"year\",\"mileage\"],\"long\":[\"groupid\"]},\"function\":\" if(goodYear.contains(year)) return (float)Math.exp(10d);   if(year==thisYear) return 87f   ; return  _INNER_SCORE    ;\",\"variables\":{\"set_int\":[\"goodYear\"],\"int\":[\"thisYear\"]}},\"values\":{\"thisYear\":2001,\"goodYear\":[1996]}}}},\"fetchStored\":false,\"from\":0,\"explain\":false,\"size\":6}";
    JSONObject res = search(new JSONObject(req));
    JSONArray hits = res.getJSONArray("hits");
    JSONObject firstHit = hits.getJSONObject(0);
    JSONObject secondHit = hits.getJSONObject(1);
    
    String firstYear = firstHit.getJSONArray("year").getString(0);
    String secondYear = secondHit.getJSONArray("year").getString(0);
    
    assertEquals("year 1996 should be on the top", true, firstYear.contains("1996"));
    assertEquals("year 1996 should be on the top", true, secondYear.contains("1996"));
  }
  
  public void testRelevanceMath() throws Exception
  {
    logger.info("executing test case testRelevanceMath");
    String req = "{\"sort\":[\"_score\"],\"query\":{\"query_string\":{\"query\":\"\",\"relevance\":{\"model\":{\"function_params\":[\"_INNER_SCORE\",\"thisYear\",\"year\",\"goodYear\"],\"facets\":{\"int\":[\"year\",\"mileage\"],\"long\":[\"groupid\"]},\"function\":\" if(goodYear.contains(year)) return (float)Math.exp(10d);   if(year==thisYear) return 87f   ; return  _INNER_SCORE    ;\",\"variables\":{\"set_int\":[\"goodYear\"],\"int\":[\"thisYear\"]}},\"values\":{\"thisYear\":2001,\"goodYear\":[1996]}}}},\"fetchStored\":false,\"from\":0,\"explain\":false,\"size\":6}";
    JSONObject res = search(new JSONObject(req));
    JSONArray hits = res.getJSONArray("hits");
    JSONObject firstHit = hits.getJSONObject(0);
    JSONObject secondHit = hits.getJSONObject(1);
    
    double firstScore = firstHit.getDouble("_score");
    double secondScore = secondHit.getDouble("_score");
    
    double delta1 = firstScore - Math.exp(10d);
    double delta2 = secondScore - Math.exp(10d);
    
    assertEquals("score for first is not correct. delta is: " + delta1, true, Math.abs(delta1) < 0.001 );
    assertEquals("score for second is not correct. delta is: " + delta2, true, Math.abs(delta2) < 0.001);
  }
  
  
  public void testRelevanceInnerScore() throws Exception
  {
    logger.info("executing test case testRelevanceInnerScore");
    String req = "{\"sort\":[\"_score\"],\"query\":{\"query_string\":{\"query\":\"\",\"relevance\":{\"model\":{\"function_params\":[\"_INNER_SCORE\",\"thisYear\",\"year\",\"goodYear\"],\"facets\":{\"int\":[\"year\",\"mileage\"],\"long\":[\"groupid\"]},\"function\":\" if(goodYear.contains(year)) return _INNER_SCORE ; return  _INNER_SCORE    ;\",\"variables\":{\"set_int\":[\"goodYear\"],\"int\":[\"thisYear\"]}},\"values\":{\"thisYear\":2001,\"goodYear\":[1996]}}}},\"fetchStored\":false,\"from\":0,\"explain\":false,\"size\":6}";
    JSONObject res = search(new JSONObject(req));
    JSONArray hits = res.getJSONArray("hits");
    JSONObject firstHit = hits.getJSONObject(0);
    JSONObject secondHit = hits.getJSONObject(1);
    
    double firstScore = firstHit.getDouble("_score");
    double secondScore = secondHit.getDouble("_score");
    
    assertEquals("inner score for first is not correct." , true, firstScore == 1 );
    assertEquals("inner score for second is not correct." , true, secondScore == 1);
  }
  
  public void testRelevanceNOW() throws Exception
  {
    logger.info("executing test case testRelevanceNOW");
    // Assume that the difference between request side "now" and node side "_NOW" is less than 2000ms.
    String req = "{\"sort\":[\"_score\"],\"query\":{\"query_string\":{\"query\":\"\",\"relevance\":{\"model\":{\"function_params\":[\"_INNER_SCORE\",\"year\",\"goodYear\",\"_NOW\",\"now\"],\"facets\":{\"int\":[\"year\",\"mileage\"],\"long\":[\"groupid\"]},\"function\":\" if(Math.abs(_NOW - now) < 2000) return 10000f; if(goodYear.contains(year)) return _INNER_SCORE ; return  _INNER_SCORE    ;\",\"variables\":{\"set_int\":[\"goodYear\"],\"int\":[\"thisYear\"],\"long\":[\"now\"]}},\"values\":{\"thisYear\":2001,\"now\":"+ System.currentTimeMillis() +",\"goodYear\":[1996]}}}},\"fetchStored\":false,\"from\":0,\"explain\":false,\"size\":6}";
    JSONObject res = search(new JSONObject(req));
    JSONArray hits = res.getJSONArray("hits");
    JSONObject firstHit = hits.getJSONObject(0);
    JSONObject secondHit = hits.getJSONObject(1);
    
    double firstScore = firstHit.getDouble("_score");
    double secondScore = secondHit.getDouble("_score");
    
    assertEquals("inner score for first is not correct." , true, firstScore == 10000f );
    assertEquals("inner score for second is not correct." , true, secondScore == 10000f);
  }
  
  public void testRelevanceStaticRandomField() throws Exception
  {
    logger.info("executing test case testRelevanceStaticRandomField");
    
    String req1 = "{\"sort\":[\"_score\"],\"query\":{\"query_string\":{\"query\":\"\",\"relevance\":{\"model\":{\"function_params\":[\"_INNER_SCORE\",\"year\",\"goodYear\",\"_NOW\",\"now\"],\"facets\":{\"int\":[\"year\",\"mileage\"],\"long\":[\"groupid\"]},\"function\":\" return _RANDOM.nextFloat()   ;\",\"variables\":{\"set_int\":[\"goodYear\"],\"int\":[\"thisYear\"],\"long\":[\"now\"]}},\"values\":{\"thisYear\":2001,\"now\":"+ System.currentTimeMillis() +",\"goodYear\":[1996]}}}},\"fetchStored\":false,\"from\":0,\"explain\":false,\"size\":6}";
    JSONObject res1 = search(new JSONObject(req1));
    JSONArray hits1 = res1.getJSONArray("hits");
    JSONObject firstHit = hits1.getJSONObject(0); 
    
    String req2 = "{\"sort\":[\"_score\"],\"query\":{\"query_string\":{\"query\":\"\",\"relevance\":{\"model\":{\"function_params\":[\"_INNER_SCORE\",\"year\",\"goodYear\",\"_NOW\",\"now\"],\"facets\":{\"int\":[\"year\",\"mileage\"],\"long\":[\"groupid\"]},\"function\":\" return _RANDOM.nextInt(10) + 10.0f   ;\",\"variables\":{\"set_int\":[\"goodYear\"],\"int\":[\"thisYear\"],\"long\":[\"now\"]}},\"values\":{\"thisYear\":2001,\"now\":"+ System.currentTimeMillis() +",\"goodYear\":[1996]}}}},\"fetchStored\":false,\"from\":0,\"explain\":false,\"size\":6}";
    JSONObject res2 = search(new JSONObject(req2));
    JSONArray hits2 = res2.getJSONArray("hits");
    JSONObject secondHit = hits2.getJSONObject(1);
    
    double firstScore = firstHit.getDouble("_score");      //  0.0f (inclusive) to 1.0f (exclusive)
    double secondScore = secondHit.getDouble("_score");   //  10.0f (inclusive) to 20.0f (exclusive)
    
    assertEquals("inner score for first is not correct." , true, (firstScore >= 0f && firstScore <1f) );
    assertEquals("inner score for second is not correct." , true, (secondScore >= 10f && secondScore < 20f));
  }
  
  public void testRelevanceHashMapInt2Float() throws Exception
  {
    logger.info("executing test case testRelevanceHashMapInt2Float");
    String req = "{\"sort\":[\"_score\"],\"query\":{\"query_string\":{\"query\":\"\",\"relevance\":{\"model\":{\"function_params\":[\"_INNER_SCORE\",\"thisYear\",\"year\",\"goodYear\",\"mileageWeight\",\"mileage\"],\"facets\":{\"int\":[\"year\",\"mileage\"],\"long\":[\"groupid\"]},\"function\":\" if(mileageWeight.containsKey(mileage)) return 10000+mileageWeight.get(mileage); if(goodYear.contains(year)) return (float)Math.exp(2d);   if(year==thisYear) return 87f   ; return  _INNER_SCORE;\",\"variables\":{\"map_int_float\":[\"mileageWeight\"],\"set_int\":[\"goodYear\"],\"int\":[\"thisYear\"]}},\"values\":{\"thisYear\":2001,\"mileageWeight\":{\"11400\":777.9, \"11000\":10.2},\"goodYear\":[1996,1997]}}}},\"fetchStored\":false,\"from\":0,\"explain\":false,\"size\":6}";
    JSONObject res = search(new JSONObject(req));
    assertEquals("numhits is wrong", 15000, res.getInt("numhits"));
    //the first one should has socre 10777.900390625, and mileage: 11400;
    JSONArray hits = res.getJSONArray("hits");
    JSONObject firstHit = hits.getJSONObject(0);
    JSONObject secondHit = hits.getJSONObject(1);
    
    double firstScore = firstHit.getDouble("_score");
    double secondScore = secondHit.getDouble("_score");
    
    String firstMileage = firstHit.getJSONArray("mileage").getString(0);
    String secondMileage = secondHit.getJSONArray("mileage").getString(0);
    
    assertEquals("inner score for first is not correct." , true, Math.abs(firstScore - 10777.900390625) < 1 );
    assertEquals("inner score for second is not correct." , true, Math.abs(secondScore - 10777.900390625) < 1 );
    
    assertEquals("mileage for first is not correct." , true, Integer.parseInt(firstMileage)==11400 );
    assertEquals("mileage for second is not correct." , true, Integer.parseInt(secondMileage)==11400 );
    
  }
  
  
  public void testRelevanceHashMapInt2String() throws Exception
  {
    logger.info("executing test case testRelevanceHashMapInt2String");
    String req = "{\"sort\":[\"_score\"],\"query\":{\"query_string\":{\"query\":\"\",\"relevance\":{\"model\":{\"function_params\":[\"_INNER_SCORE\",\"thisYear\",\"year\",\"goodYear\",\"mileageWeight\",\"mileage\",\"color\",\"yearcolor\",\"colorweight\",\"category\",\"categorycolor\"],\"facets\":{\"int\":[\"year\",\"mileage\"],\"string\":[\"color\",\"category\"],\"long\":[\"groupid\"]},\"function\":\"if(yearcolor.containsKey(year) && yearcolor.get(year).equals(color)) return 100000f; if(goodYear.contains(year)) return (float)Math.exp(2d);   if(year==thisYear) return 87f   ; return  _INNER_SCORE;\",\"variables\":{\"map_int_float\":[\"mileageWeight\"],\"map_int_string\":[\"yearcolor\"],\"set_int\":[\"goodYear\"],\"int\":[\"thisYear\"],\"map_string_float\":[\"colorweight\"],\"map_string_string\":[\"categorycolor\"]}},\"values\":{\"thisYear\":2001,\"yearcolor\":{\"1998\":\"red\"},\"mileageWeight\":{\"11400\":777.9, \"11000\":10.2},\"colorweight\":{\"red\":335.5},\"goodYear\":[1996,1997],\"categorycolor\":{\"compact\":\"red\"}}}}},\"fetchStored\":false,\"from\":0,\"explain\":false,\"size\":6}";    
    JSONObject res = search(new JSONObject(req));
    assertEquals("numhits is wrong", 15000, res.getInt("numhits"));
    //the first one should has socre 10777.900390625, and mileage: 11400;
    JSONArray hits = res.getJSONArray("hits");
    JSONObject firstHit = hits.getJSONObject(0);
    JSONObject secondHit = hits.getJSONObject(1);
    
    double firstScore = firstHit.getDouble("_score");
    double secondScore = secondHit.getDouble("_score");
    
    String firstYear = firstHit.getJSONArray("year").getString(0);
    String secondYear = secondHit.getJSONArray("year").getString(0);
    
    String firstColor = firstHit.getJSONArray("color").getString(0);
    String secondColor = secondHit.getJSONArray("color").getString(0);
    
    assertEquals("inner score for first is not correct." , true, Math.abs(firstScore - 100000) < 1 );
    assertEquals("inner score for second is not correct." , true, Math.abs(secondScore - 100000) < 1 );
    
    assertEquals("year for first is not correct." , true, Integer.parseInt(firstYear)==1998 );
    assertEquals("year for second is not correct." , true, Integer.parseInt(secondYear)==1998 );
    
    assertEquals("color for first is not correct." , true, firstColor.equals("red") );
    assertEquals("color for second is not correct." , true, secondColor.equals("red") );
    
  }
  
  public void testRelevanceHashMapString2Float() throws Exception
  {
    logger.info("executing test case testRelevanceHashMapString2Float");
    String req = "{\"sort\":[\"_score\"],\"query\":{\"query_string\":{\"query\":\"\",\"relevance\":{\"model\":{\"function_params\":[\"_INNER_SCORE\",\"thisYear\",\"year\",\"goodYear\",\"mileageWeight\",\"mileage\",\"color\",\"yearcolor\",\"colorweight\",\"category\",\"categorycolor\"],\"facets\":{\"int\":[\"year\",\"mileage\"],\"string\":[\"color\",\"category\"],\"long\":[\"groupid\"]},\"function\":\" if(colorweight.containsKey(color) ) return 200f + colorweight.getFloat(color);  if(year==thisYear) return 87f   ; return  _INNER_SCORE;\",\"variables\":{\"map_int_float\":[\"mileageWeight\"],\"map_int_string\":[\"yearcolor\"],\"set_int\":[\"goodYear\"],\"int\":[\"thisYear\"],\"map_string_float\":[\"colorweight\"],\"map_string_string\":[\"categorycolor\"]}},\"values\":{\"thisYear\":2001,\"yearcolor\":{\"1998\":\"red\"},\"mileageWeight\":{\"11400\":777.9, \"11000\":10.2},\"colorweight\":{\"red\":335.5},\"goodYear\":[1996,1997],\"categorycolor\":{\"compact\":\"red\"}}}}},\"fetchStored\":false,\"from\":0,\"explain\":false,\"size\":6}";
    JSONObject res = search(new JSONObject(req));
    assertEquals("numhits is wrong", 15000, res.getInt("numhits"));
    //the first one should has socre 10777.900390625, and mileage: 11400;
    JSONArray hits = res.getJSONArray("hits");
    JSONObject firstHit = hits.getJSONObject(0);
    JSONObject secondHit = hits.getJSONObject(1);
    
    double firstScore = firstHit.getDouble("_score");
    double secondScore = secondHit.getDouble("_score");
    
    String firstColor = firstHit.getJSONArray("color").getString(0);
    String secondColor = secondHit.getJSONArray("color").getString(0);
    
    assertEquals("inner score for first is not correct." , true, Math.abs(firstScore - 535.5) < 1 );
    assertEquals("inner score for second is not correct." , true, Math.abs(secondScore - 535.5) < 1 );
    
    assertEquals("color for first is not correct." , true, firstColor.equals("red") );
    assertEquals("color for second is not correct." , true, secondColor.equals("red") );
    
  }
  
  public void testRelevanceHashMapString2String() throws Exception
  {
    logger.info("executing test case testRelevanceHashMapString2String");
    String req = "{\"sort\":[\"_score\"],\"query\":{\"query_string\":{\"query\":\"\",\"relevance\":{\"model\":{\"function_params\":[\"_INNER_SCORE\",\"thisYear\",\"year\",\"goodYear\",\"mileageWeight\",\"mileage\",\"color\",\"yearcolor\",\"colorweight\",\"category\",\"categorycolor\"],\"facets\":{\"int\":[\"year\",\"mileage\"],\"string\":[\"color\",\"category\"],\"long\":[\"groupid\"]},\"function\":\" if(categorycolor.containsKey(category) && categorycolor.get(category).equals(color))  return 10000f;   if(year==thisYear) return 87f   ; return  _INNER_SCORE;\",\"variables\":{\"map_int_float\":[\"mileageWeight\"],\"map_int_string\":[\"yearcolor\"],\"set_int\":[\"goodYear\"],\"int\":[\"thisYear\"],\"map_string_float\":[\"colorweight\"],\"map_string_string\":[\"categorycolor\"]}},\"values\":{\"thisYear\":2001,\"yearcolor\":{\"1998\":\"red\"},\"mileageWeight\":{\"11400\":777.9, \"11000\":10.2},\"colorweight\":{\"red\":335.5},\"goodYear\":[1996,1997],\"categorycolor\":{\"compact\":\"red\"}}}}},\"fetchStored\":false,\"from\":0,\"explain\":false,\"size\":6}";
    JSONObject res = search(new JSONObject(req));
    assertEquals("numhits is wrong", 15000, res.getInt("numhits"));
    //the first one should has socre 10777.900390625, and mileage: 11400;
    JSONArray hits = res.getJSONArray("hits");
    JSONObject firstHit = hits.getJSONObject(0);
    JSONObject secondHit = hits.getJSONObject(1);
    
    double firstScore = firstHit.getDouble("_score");
    double secondScore = secondHit.getDouble("_score");
    
    String firstCategory = firstHit.getJSONArray("category").getString(0);
    String secondCategory = secondHit.getJSONArray("category").getString(0);
    
    String firstColor = firstHit.getJSONArray("color").getString(0);
    String secondColor = secondHit.getJSONArray("color").getString(0);
    
    assertEquals("inner score for first is not correct." , true, Math.abs(firstScore - 10000) < 1 );
    assertEquals("inner score for second is not correct." , true, Math.abs(secondScore - 10000) < 1 );
    
    assertEquals("category for first is not correct." , true, firstCategory.equals("compact") );
    assertEquals("category for second is not correct." , true, secondCategory.equals("compact") );
    
    assertEquals("color for first is not correct." , true, firstColor.equals("red") );
    assertEquals("color for second is not correct." , true, secondColor.equals("red") );
  }
  
  public void testRelevanceHashMapInt2FloatArrayWay() throws Exception
  {
    logger.info("executing test case testRelevanceHashMapInt2FloatArrayWay");
    String req = "{\"sort\":[\"_score\"],\"query\":{\"query_string\":{\"query\":\"\",\"relevance\":{\"model\":{\"function_params\":[\"_INNER_SCORE\",\"thisYear\",\"year\",\"goodYear\",\"mileageWeight\",\"mileage\"],\"facets\":{\"int\":[\"year\",\"mileage\"],\"long\":[\"groupid\"]},\"function\":\" if(mileageWeight.containsKey(mileage)) return 10000+mileageWeight.get(mileage); if(goodYear.contains(year)) return (float)Math.exp(2d);   if(year==thisYear) return 87f   ; return  _INNER_SCORE;\",\"variables\":{\"map_int_float\":[\"mileageWeight\"],\"set_int\":[\"goodYear\"],\"int\":[\"thisYear\"]}},\"values\":{\"thisYear\":2001,\"mileageWeight\":{\"value\":[777.9,10.2],\"key\":[11400,11000]},\"goodYear\":[1996,1997]}}}},\"fetchStored\":false,\"from\":0,\"explain\":false,\"size\":6}";
    JSONObject res = search(new JSONObject(req));
    assertEquals("numhits is wrong", 15000, res.getInt("numhits"));
    //the first one should has socre 10777.900390625, and mileage: 11400;
    JSONArray hits = res.getJSONArray("hits");
    JSONObject firstHit = hits.getJSONObject(0);
    JSONObject secondHit = hits.getJSONObject(1);
    
    double firstScore = firstHit.getDouble("_score");
    double secondScore = secondHit.getDouble("_score");
    
    String firstMileage = firstHit.getJSONArray("mileage").getString(0);
    String secondMileage = secondHit.getJSONArray("mileage").getString(0);
    
    assertEquals("inner score for first is not correct." , true, Math.abs(firstScore - 10777.900390625) < 1 );
    assertEquals("inner score for second is not correct." , true, Math.abs(secondScore - 10777.900390625) < 1 );
    
    assertEquals("mileage for first is not correct." , true, Integer.parseInt(firstMileage)==11400 );
    assertEquals("mileage for second is not correct." , true, Integer.parseInt(secondMileage)==11400 );
    
  }
  
  
  public void testRelevanceHashMapInt2StringArrayWay() throws Exception
  {
    logger.info("executing test case testRelevanceHashMapInt2StringArrayWay");
    String req = "{\"sort\":[\"_score\"],\"query\":{\"query_string\":{\"query\":\"\",\"relevance\":{\"model\":{\"function_params\":[\"_INNER_SCORE\",\"thisYear\",\"year\",\"goodYear\",\"mileageWeight\",\"mileage\",\"color\",\"yearcolor\",\"colorweight\",\"category\",\"categorycolor\"],\"facets\":{\"int\":[\"year\",\"mileage\"],\"string\":[\"color\",\"category\"],\"long\":[\"groupid\"]},\"function\":\"if(yearcolor.containsKey(year) && yearcolor.get(year).equals(color)) return 100000f; if(goodYear.contains(year)) return (float)Math.exp(2d);   if(year==thisYear) return 87f   ; return  _INNER_SCORE;\",\"variables\":{\"map_int_float\":[\"mileageWeight\"],\"map_int_string\":[\"yearcolor\"],\"set_int\":[\"goodYear\"],\"int\":[\"thisYear\"],\"map_string_float\":[\"colorweight\"],\"map_string_string\":[\"categorycolor\"]}},\"values\":{\"thisYear\":2001,\"yearcolor\":{\"value\":[\"red\"],\"key\":[1998]},\"mileageWeight\":{\"value\":[777.9,10.2],\"key\":[11400,11000]},\"colorweight\":{\"value\":[335.5],\"key\":[\"red\"]},\"goodYear\":[1996,1997],\"categorycolor\":{\"value\":[\"red\"],\"key\":[\"compact\"]}}}}},\"fetchStored\":false,\"from\":0,\"explain\":false,\"size\":6}";
    JSONObject res = search(new JSONObject(req));
    assertEquals("numhits is wrong", 15000, res.getInt("numhits"));
    //the first one should has socre 10777.900390625, and mileage: 11400;
    JSONArray hits = res.getJSONArray("hits");
    JSONObject firstHit = hits.getJSONObject(0);
    JSONObject secondHit = hits.getJSONObject(1);
    
    double firstScore = firstHit.getDouble("_score");
    double secondScore = secondHit.getDouble("_score");
    
    String firstYear = firstHit.getJSONArray("year").getString(0);
    String secondYear = secondHit.getJSONArray("year").getString(0);
    
    String firstColor = firstHit.getJSONArray("color").getString(0);
    String secondColor = secondHit.getJSONArray("color").getString(0);
    
    assertEquals("inner score for first is not correct." , true, Math.abs(firstScore - 100000) < 1 );
    assertEquals("inner score for second is not correct." , true, Math.abs(secondScore - 100000) < 1 );
    
    assertEquals("year for first is not correct." , true, Integer.parseInt(firstYear)==1998 );
    assertEquals("year for second is not correct." , true, Integer.parseInt(secondYear)==1998 );
    
    assertEquals("color for first is not correct." , true, firstColor.equals("red") );
    assertEquals("color for second is not correct." , true, secondColor.equals("red") );
    
  }
  
  public void testRelevanceHashMapString2FloatArrayWay() throws Exception
  {
    logger.info("executing test case testRelevanceHashMapString2FloatArrayWay");
    String req = "{\"sort\":[\"_score\"],\"query\":{\"query_string\":{\"query\":\"\",\"relevance\":{\"model\":{\"function_params\":[\"_INNER_SCORE\",\"thisYear\",\"year\",\"goodYear\",\"mileageWeight\",\"mileage\",\"color\",\"yearcolor\",\"colorweight\",\"category\",\"categorycolor\"],\"facets\":{\"int\":[\"year\",\"mileage\"],\"string\":[\"color\",\"category\"],\"long\":[\"groupid\"]},\"function\":\" if(colorweight.containsKey(color) ) return 200f + colorweight.getFloat(color);  if(year==thisYear) return 87f   ; return  _INNER_SCORE;\",\"variables\":{\"map_int_float\":[\"mileageWeight\"],\"map_int_string\":[\"yearcolor\"],\"set_int\":[\"goodYear\"],\"int\":[\"thisYear\"],\"map_string_float\":[\"colorweight\"],\"map_string_string\":[\"categorycolor\"]}},\"values\":{\"thisYear\":2001,\"yearcolor\":{\"value\":[\"red\"],\"key\":[1998]},\"mileageWeight\":{\"value\":[777.9,10.2],\"key\":[11400,11000]},\"colorweight\":{\"value\":[335.5],\"key\":[\"red\"]},\"goodYear\":[1996,1997],\"categorycolor\":{\"value\":[\"red\"],\"key\":[\"compact\"]}}}}},\"fetchStored\":false,\"from\":0,\"explain\":false,\"size\":6}";
    JSONObject res = search(new JSONObject(req));
    assertEquals("numhits is wrong", 15000, res.getInt("numhits"));
    //the first one should has socre 10777.900390625, and mileage: 11400;
    JSONArray hits = res.getJSONArray("hits");
    JSONObject firstHit = hits.getJSONObject(0);
    JSONObject secondHit = hits.getJSONObject(1);
    
    double firstScore = firstHit.getDouble("_score");
    double secondScore = secondHit.getDouble("_score");
    
    String firstColor = firstHit.getJSONArray("color").getString(0);
    String secondColor = secondHit.getJSONArray("color").getString(0);
    
    assertEquals("inner score for first is not correct." , true, Math.abs(firstScore - 535.5) < 1 );
    assertEquals("inner score for second is not correct." , true, Math.abs(secondScore - 535.5) < 1 );
    
    assertEquals("color for first is not correct." , true, firstColor.equals("red") );
    assertEquals("color for second is not correct." , true, secondColor.equals("red") );
    
  }
  
  public void testRelevanceHashMapString2StringArrayWay() throws Exception
  {
    logger.info("executing test case testRelevanceHashMapString2StringArrayWay");
    String req = "{\"sort\":[\"_score\"],\"query\":{\"query_string\":{\"query\":\"\",\"relevance\":{\"model\":{\"function_params\":[\"_INNER_SCORE\",\"thisYear\",\"year\",\"goodYear\",\"mileageWeight\",\"mileage\",\"color\",\"yearcolor\",\"colorweight\",\"category\",\"categorycolor\"],\"facets\":{\"int\":[\"year\",\"mileage\"],\"string\":[\"color\",\"category\"],\"long\":[\"groupid\"]},\"function\":\" if(categorycolor.containsKey(category) && categorycolor.get(category).equals(color))  return 10000f;   if(year==thisYear) return 87f   ; return  _INNER_SCORE;\",\"variables\":{\"map_int_float\":[\"mileageWeight\"],\"map_int_string\":[\"yearcolor\"],\"set_int\":[\"goodYear\"],\"int\":[\"thisYear\"],\"map_string_float\":[\"colorweight\"],\"map_string_string\":[\"categorycolor\"]}},\"values\":{\"thisYear\":2001,\"yearcolor\":{\"value\":[\"red\"],\"key\":[1998]},\"mileageWeight\":{\"value\":[777.9,10.2],\"key\":[11400,11000]},\"colorweight\":{\"value\":[335.5],\"key\":[\"red\"]},\"goodYear\":[1996,1997],\"categorycolor\":{\"value\":[\"red\"],\"key\":[\"compact\"]}}}}},\"fetchStored\":false,\"from\":0,\"explain\":false,\"size\":6}";
    JSONObject res = search(new JSONObject(req));
    assertEquals("numhits is wrong", 15000, res.getInt("numhits"));
    //the first one should has socre 10777.900390625, and mileage: 11400;
    JSONArray hits = res.getJSONArray("hits");
    JSONObject firstHit = hits.getJSONObject(0);
    JSONObject secondHit = hits.getJSONObject(1);
    
    double firstScore = firstHit.getDouble("_score");
    double secondScore = secondHit.getDouble("_score");
    
    String firstCategory = firstHit.getJSONArray("category").getString(0);
    String secondCategory = secondHit.getJSONArray("category").getString(0);
    
    String firstColor = firstHit.getJSONArray("color").getString(0);
    String secondColor = secondHit.getJSONArray("color").getString(0);
    
    assertEquals("inner score for first is not correct." , true, Math.abs(firstScore - 10000) < 1 );
    assertEquals("inner score for second is not correct." , true, Math.abs(secondScore - 10000) < 1 );
    
    assertEquals("category for first is not correct." , true, firstCategory.equals("compact") );
    assertEquals("category for second is not correct." , true, secondCategory.equals("compact") );
    
    assertEquals("color for first is not correct." , true, firstColor.equals("red") );
    assertEquals("color for second is not correct." , true, secondColor.equals("red") );
  }
  
  
  public void testRelevanceMultiContains() throws Exception
  {
    logger.info("executing test case testRelevanceMultiContains");
    String req = "{\"sort\":[\"_score\"],\"query\":{\"query_string\":{\"query\":\"\",\"relevance\":{\"model\":{\"function_params\":[\"_INNER_SCORE\",\"thisYear\",\"year\",\"goodYear\",\"tags\",\"coolTag\"],\"facets\":{\"mstring\":[\"tags\"],\"int\":[\"year\",\"mileage\"],\"long\":[\"groupid\"]},\"function\":\" if(tags.contains(coolTag)) return 999999f; if(goodYear.contains(year)) return (float)Math.exp(10d);   if(year==thisYear) return 87f   ; return  _INNER_SCORE    ;\",\"variables\":{\"set_int\":[\"goodYear\"],\"int\":[\"thisYear\"], \"string\":[\"coolTag\"]}},\"values\":{\"coolTag\":\"cool\", \"thisYear\":2001,\"goodYear\":[1996,1997]}}}},\"fetchStored\":false,\"from\":0,\"explain\":false,\"size\":6}";
    JSONObject res = search(new JSONObject(req));
    assertEquals("numhits is wrong", 15000, res.getInt("numhits"));
  }
  
  
  
  public void testRelevanceMultiContainsAny() throws Exception
  {
    logger.info("executing test case testRelevanceMultiContainsAny");
    String req = "{\"sort\":[\"_score\"],\"query\":{\"query_string\":{\"query\":\"\",\"relevance\":{\"model\":{\"function_params\":[\"_INNER_SCORE\",\"thisYear\",\"year\",\"goodYear\",\"tags\",\"goodTags\"],\"facets\":{\"int\":[\"year\",\"mileage\"],\"mstring\":[\"tags\"],\"long\":[\"groupid\"]},\"function\":\" if(tags.containsAny(goodTags)) return 100000f; if(goodYear.contains(year)) return (float)Math.exp(10d);   if(year==thisYear) return 87f   ; return  _INNER_SCORE    ;\",\"variables\":{\"set_string\":[\"goodTags\"],\"set_int\":[\"goodYear\"],\"int\":[\"thisYear\"]}},\"values\":{\"thisYear\":2001,\"goodTags\":[\"leather\"],\"goodYear\":[1996,1997]}}}},\"fetchStored\":false,\"from\":0,\"explain\":false,\"size\":6}";
    JSONObject res = search(new JSONObject(req));
    assertEquals("numhits is wrong", 15000, res.getInt("numhits"));

    JSONArray hits = res.getJSONArray("hits");
    JSONObject firstHit = hits.getJSONObject(0);
    JSONObject secondHit = hits.getJSONObject(1);
    
    double firstScore = firstHit.getDouble("_score");
    double secondScore = secondHit.getDouble("_score");
    
    JSONArray firstTags = firstHit.getJSONArray("tags");
    JSONArray secondTags = secondHit.getJSONArray("tags");
    
    assertEquals("inner score for first is not correct." , true, Math.abs(firstScore - 100000) < 1 );
    assertEquals("inner score for second is not correct." , true, Math.abs(secondScore - 100000) < 1 );
    
    assertEquals("tags for first is not correct." , true, containsString(firstTags, "leather") );
    assertEquals("tags for second is not correct." , true, containsString(secondTags, "leather") );
  }
  
  public void testRelevanceModelStorageInMemory() throws Exception
  {
    logger.info("executing test case testRelevanceModelStorageInMemory");
    
    // store the model;
    {
      String req = "{\"sort\":[\"_score\"],\"query\":{\"query_string\":{\"query\":\"\",\"relevance\":{\"model\":{\"save_as\":{\"overwrite\":true,\"name\":\"myModel\"},\"function_params\":[\"_INNER_SCORE\",\"thisYear\",\"year\",\"goodYear\",\"mileageWeight\",\"mileage\",\"color\",\"yearcolor\",\"colorweight\",\"category\",\"categorycolor\"],\"facets\":{\"int\":[\"year\",\"mileage\"],\"string\":[\"color\",\"category\"],\"long\":[\"groupid\"]},\"function\":\" if(categorycolor.containsKey(category) && categorycolor.get(category).equals(color))  return 10000f; if(colorweight.containsKey(color) ) return 200f + colorweight.getFloat(color); if(yearcolor.containsKey(year) && yearcolor.get(year).equals(color)) return 200f; if(mileageWeight.containsKey(mileage)) return 10000+mileageWeight.get(mileage); if(goodYear.contains(year)) return (float)Math.exp(2d);   if(year==thisYear) return 87f   ; return  _INNER_SCORE;\",\"variables\":{\"map_int_float\":[\"mileageWeight\"],\"map_int_string\":[\"yearcolor\"],\"set_int\":[\"goodYear\"],\"int\":[\"thisYear\"],\"map_string_float\":[\"colorweight\"],\"map_string_string\":[\"categorycolor\"]}},\"values\":{\"thisYear\":2001,\"yearcolor\":{\"value\":[\"red\"],\"key\":[1998]},\"mileageWeight\":{\"value\":[777.9,10.2],\"key\":[11400,11000]},\"colorweight\":{\"value\":[335.5],\"key\":[\"red\"]},\"goodYear\":[1996,1997],\"categorycolor\":{\"value\":[\"red\"],\"key\":[\"compact\"]}}}}},\"fetchStored\":false,\"from\":0,\"explain\":false,\"size\":6}";
      JSONObject res = search(new JSONObject(req));
      assertEquals("numhits is wrong", 15000, res.getInt("numhits"));

      JSONArray hits = res.getJSONArray("hits");
      JSONObject firstHit = hits.getJSONObject(0);
      JSONObject secondHit = hits.getJSONObject(1);
      
      double firstScore = firstHit.getDouble("_score");
      double secondScore = secondHit.getDouble("_score");
      
      assertEquals("inner score for first is not correct." , true, Math.abs(firstScore - 10777.9) < 1 );
      assertEquals("inner score for second is not correct." , true, Math.abs(secondScore - 10777.9) < 1 );
    }
    
    
    // assuming the model is already stored, test new query using only stored model name;
    {
      String req = "{\"sort\":[\"_score\"],\"query\":{\"query_string\":{\"query\":\"\",\"relevance\":{\"predefined_model\":\"myModel\",\"values\":{\"thisYear\":2001,\"yearcolor\":{\"value\":[\"red\"],\"key\":[1998]},\"mileageWeight\":{\"value\":[777.9,10.2],\"key\":[11400,11000]},\"colorweight\":{\"value\":[335.5],\"key\":[\"red\"]},\"goodYear\":[1996,1997],\"categorycolor\":{\"value\":[\"red\"],\"key\":[\"compact\"]}}}}},\"fetchStored\":false,\"from\":0,\"explain\":false,\"size\":6}";
      JSONObject res = search(new JSONObject(req));
      assertEquals("numhits is wrong", 15000, res.getInt("numhits"));

      JSONArray hits = res.getJSONArray("hits");
      JSONObject firstHit = hits.getJSONObject(0);
      JSONObject secondHit = hits.getJSONObject(1);
      
      double firstScore = firstHit.getDouble("_score");
      double secondScore = secondHit.getDouble("_score");
      
      assertEquals("inner score for first is not correct." , true, Math.abs(firstScore - 10777.9) < 1 );
      assertEquals("inner score for second is not correct." , true, Math.abs(secondScore - 10777.9) < 1 );
    }
    
  }
  
  public void testRelevanceExternalObject() throws Exception
  {
    logger.info("executing test case testRelevanceExternalObject");
    String req = "{\"sort\":[\"_score\"],\"query\":{\"query_string\":{\"query\":\"\",\"relevance\":{\"model\":{\"function_params\":[\"_INNER_SCORE\",\"thisYear\",\"year\",\"goodYear\",\"mileageWeight\",\"mileage\",\"color\",\"yearcolor\",\"colorweight\",\"category\",\"categorycolor\",\"test_obj2\"],\"facets\":{\"int\":[\"year\",\"mileage\"],\"string\":[\"color\",\"category\"],\"long\":[\"groupid\"]},\"function\":\" if(test_obj2.contains(color)) return 20000f; if(categorycolor.containsKey(category) && categorycolor.get(category).equals(color))  return 10000f; if(colorweight.containsKey(color) ) return 200f + colorweight.getFloat(color); if(yearcolor.containsKey(year) && yearcolor.get(year).equals(color)) return 200f; if(mileageWeight.containsKey(mileage)) return 10000+mileageWeight.get(mileage); if(goodYear.contains(year)) return (float)Math.exp(2d);   if(year==thisYear) return 87f   ; return  _INNER_SCORE;\",\"variables\":{\"map_int_float\":[\"mileageWeight\"],\"map_int_string\":[\"yearcolor\"],\"set_int\":[\"goodYear\"],\"custom_obj\":[\"test_obj2\"],\"int\":[\"thisYear\"],\"map_string_float\":[\"colorweight\"],\"map_string_string\":[\"categorycolor\"]}},\"values\":{\"thisYear\":2001,\"yearcolor\":{\"value\":[\"red\"],\"key\":[1998]},\"mileageWeight\":{\"value\":[777.9,10.2],\"key\":[11400,11000]},\"colorweight\":{\"value\":[335.5],\"key\":[\"red\"]},\"goodYear\":[1996,1997],\"categorycolor\":{\"value\":[\"red\"],\"key\":[\"compact\"]}}}}},\"fetchStored\":false,\"from\":0,\"explain\":false,\"size\":2}";
    JSONObject res = search(new JSONObject(req));
    assertEquals("numhits is wrong", 15000, res.getInt("numhits"));

    JSONArray hits = res.getJSONArray("hits");
    JSONObject firstHit = hits.getJSONObject(0);
    double firstScore = firstHit.getDouble("_score");
    String color = firstHit.getJSONArray("color").getString(0);
    
    assertEquals("color for first is not correct." , true, color.equals("green") );
    assertEquals("inner score for first is not correct." , true, Math.abs(firstScore - 20000) < 1 );
  }
  
  public void testRelevanceExternalObjectSenseiPlugin() throws Exception
  {
    logger.info("executing test case testRelevanceExternalObjectSenseiPlugin");
    String req = "{\"sort\":[\"_score\"],\"query\":{\"query_string\":{\"query\":\"\",\"relevance\":{\"model\":{\"function_params\":[\"_INNER_SCORE\",\"thisYear\",\"year\",\"goodYear\",\"mileageWeight\",\"mileage\",\"color\",\"yearcolor\",\"colorweight\",\"category\",\"categorycolor\",\"test_obj\"],\"facets\":{\"int\":[\"year\",\"mileage\"],\"string\":[\"color\",\"category\"],\"long\":[\"groupid\"]},\"function\":\" if(test_obj.contains(color)) return 20000f; if(categorycolor.containsKey(category) && categorycolor.get(category).equals(color))  return 10000f; if(colorweight.containsKey(color) ) return 200f + colorweight.getFloat(color); if(yearcolor.containsKey(year) && yearcolor.get(year).equals(color)) return 200f; if(mileageWeight.containsKey(mileage)) return 10000+mileageWeight.get(mileage); if(goodYear.contains(year)) return (float)Math.exp(2d);   if(year==thisYear) return 87f   ; return  _INNER_SCORE;\",\"variables\":{\"map_int_float\":[\"mileageWeight\"],\"map_int_string\":[\"yearcolor\"],\"set_int\":[\"goodYear\"],\"custom_obj\":[\"test_obj\"],\"int\":[\"thisYear\"],\"map_string_float\":[\"colorweight\"],\"map_string_string\":[\"categorycolor\"]}},\"values\":{\"thisYear\":2001,\"yearcolor\":{\"value\":[\"red\"],\"key\":[1998]},\"mileageWeight\":{\"value\":[777.9,10.2],\"key\":[11400,11000]},\"colorweight\":{\"value\":[335.5],\"key\":[\"red\"]},\"goodYear\":[1996,1997],\"categorycolor\":{\"value\":[\"red\"],\"key\":[\"compact\"]}}}}},\"fetchStored\":false,\"from\":0,\"explain\":false,\"size\":2}";
    JSONObject res = search(new JSONObject(req));
    assertEquals("numhits is wrong", 15000, res.getInt("numhits"));

    JSONArray hits = res.getJSONArray("hits");
    JSONObject firstHit = hits.getJSONObject(0);
    double firstScore = firstHit.getDouble("_score");
    String color = firstHit.getJSONArray("color").getString(0);
    
    assertEquals("color for first is not correct." , true, color.equals("red") );
    assertEquals("inner score for first is not correct." , true, Math.abs(firstScore - 20000) < 1 );
  }
  

  public void testRelevanceWeightedMulti() throws Exception
  {
    logger.info("executing test case testRelevanceNOW");
    // Assume that the difference between request side "now" and node side "_NOW" is less than 2000ms.
    String req = "{\"sort\":[\"_score\"],\"query\":{\"query_string\":{\"query\":\"\",\"relevance\":{\"model\":{\"function_params\":[\"_INNER_SCORE\",\"wtags\",\"goodtag\"],\"facets\":{\"wmstring\":[\"wtags\"]},\"function\":\" if(wtags.hasWeight(goodtag))    _INNER_SCORE = wtags.getWeight();  return  _INNER_SCORE;\",\"variables\":{\"string\":[\"goodtag\"]}},\"values\":{\"goodtag\":\"reliable\"}}}},\"fetchStored\":true,\"from\":0,\"explain\":true,\"size\":10}";
    JSONObject res = search(new JSONObject(req));
    int numhits = res.getInt("numhits");
    assertTrue("numhits is wrong. get "+ numhits, res.getInt("numhits") == 15000);
    logger.info("request:" + req + "\nresult:" + res);
    
    
    JSONArray hits = res.getJSONArray("hits");
    JSONObject firstHit = hits.getJSONObject(0);
    JSONObject secondHit = hits.getJSONObject(1);
    
    double firstScore = firstHit.getDouble("_score");
    double secondScore = secondHit.getDouble("_score");
    assertEquals("inner score for first is not correct." , true, Math.abs(firstScore - 999) < 1 );
    assertEquals("inner score for second is not correct." , true, Math.abs(secondScore - 1) < 0.1 );
  }
  
  private boolean containsString(JSONArray array, String target) throws JSONException
  {
    for(int i=0; i<array.length(); i++)
    {
      String item = array.getString(i);
      if(item.equals(target))
        return true;
    }
    return false;
  }

  public static JSONObject search(JSONObject req) throws Exception  {
    return  search(SenseiStarter.SenseiUrl, req.toString());
  }
  public static JSONObject searchGet(JSONArray req) throws Exception  {
    return  search(new URL(SenseiStarter.SenseiUrl.toString() + "/get"), req.toString());
  }
  public static JSONObject search(URL url, String req) throws Exception {
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
    // System.out.println("res: " + res);
    res = res.replace('\u0000', '*');  // replace the seperator for test case;
    JSONObject ret = new JSONObject(res);
    if (ret.opt("totaldocs") !=null){
     // assertEquals(15000L, ret.getLong("totaldocs"));
    }
    return ret;
  }

  private void setspec(SenseiRequest req, FacetSpec spec) {
    req.setFacetSpec("color", spec);
    req.setFacetSpec("category", spec);
    req.setFacetSpec("city", spec);
    req.setFacetSpec("makemodel", spec);
    req.setFacetSpec("year", spec);
    req.setFacetSpec("price", spec);
    req.setFacetSpec("mileage", spec);
    req.setFacetSpec("tags", spec);
  }







//  public void testSortBy() throws Exception
//  {
//    logger.info("executing test case testSortBy");
//    String req = "{\"sort\":[{\"color\":\"desc\"},\"_score\"],\"from\":0,\"size\":15000}";
//    JSONObject res = search(new JSONObject(req));
//    JSONArray jhits = res.optJSONArray("hits");
//    ArrayList<String> arColors = new ArrayList<String>();
//    for(int i=0; i<jhits.length(); i++){
//      JSONObject jhit = jhits.getJSONObject(i);
//      JSONArray jcolor = jhit.optJSONArray("color");
//      if(jcolor != null){
//        String color = jcolor.optString(0);
//        if(color != null)
//          arColors.add(color);
//      }
//    }
//    checkColorOrder(arColors);
//    //    assertEquals("numhits is wrong", 15000, res.getInt("numhits"));
//  }

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


  public void testSortByDesc() throws Exception

  {
    logger.info("executing test case testSortByDesc");
    String req = "{\"selections\": [{\"range\": {\"mileage\": {\"from\": 16000, \"include_lower\": false}}}, {\"range\": {\"year\": {\"from\": 2002, \"include_lower\": true, \"include_upper\": true, \"to\": 2002}}}], \"sort\":[{\"color\":\"desc\"}, {\"category\":\"asc\"}],\"from\":0,\"size\":15000}";
    JSONObject res = search(new JSONObject(req));
    JSONArray jhits = res.optJSONArray("hits");
    ArrayList<String> arColors = new ArrayList<String>();
    ArrayList<String> arCategories = new ArrayList<String>();
    for(int i=0; i<jhits.length(); i++){
      JSONObject jhit = jhits.getJSONObject(i);
      JSONArray jcolor = jhit.optJSONArray("color");
      if(jcolor != null){
        String color = jcolor.optString(0);
        if(color != null)
          arColors.add(color);
      }
      JSONArray jcategory = jhit.optJSONArray("category");
      if (jcategory != null)
      {
        String category = jcategory.optString(0);
        if (category != null)
        {
          arCategories.add(category);
        }
      }
    }
    checkOrder(arColors, arCategories, true, false);
  }

  public void testSortByAsc() throws Exception
  {
    logger.info("executing test case testSortByAsc");
    String req = "{\"selections\": [{\"range\": {\"mileage\": {\"from\": 16000, \"include_lower\": false}}}, {\"range\": {\"year\": {\"from\": 2002, \"include_lower\": true, \"include_upper\": true, \"to\": 2002}}}], \"sort\":[{\"color\":\"asc\"}, {\"category\":\"desc\"}],\"from\":0,\"size\":15000}";
    JSONObject res = search(new JSONObject(req));
    JSONArray jhits = res.optJSONArray("hits");
    ArrayList<String> arColors = new ArrayList<String>();
    ArrayList<String> arCategories = new ArrayList<String>();
    for(int i=0; i<jhits.length(); i++){
      JSONObject jhit = jhits.getJSONObject(i);
      JSONArray jcolor = jhit.optJSONArray("color");
      if(jcolor != null){
        String color = jcolor.optString(0);
        if(color != null)
          arColors.add(color);
      }
      JSONArray jcategory = jhit.optJSONArray("category");
      if (jcategory != null)
      {
        String category = jcategory.optString(0);
        if (category != null)
        {
          arCategories.add(category);
        }
      }
    }
    checkOrder(arColors, arCategories, false, true);
  }

  private void checkOrder(ArrayList<String> arColors,
                          ArrayList<String> arCategories,
                          boolean colorDesc,
                          boolean categoryDesc)
  {
    assertEquals("Color array and category array must have same size",
                 arColors.size(), arCategories.size());
    assertTrue("must have 3680 results, size is:" + arColors.size(), arColors.size() == 368);
    for(int i=0; i< arColors.size()-1; i++){
      String first = arColors.get(i);
      String next = arColors.get(i+1);
      String firstCategory = arCategories.get(i);
      String nextCategory = arCategories.get(i+1);

      // System.out.println(">>> color = " + first + ", category = " + firstCategory);

      int comp = first.compareTo(next);
      if (colorDesc)
      {
        assertTrue("should >=0 (first= "+ first+"  next= "+ next+")", comp>=0);
      }
      else
      {
        assertTrue("should <=0 (first= "+ first+"  next= "+ next+")", comp<=0);
      }
      if (comp == 0)
      {
        int compCategory = firstCategory.compareTo(nextCategory);
        if (categoryDesc)
        {
          assertTrue("should >=0 (firstCategory= "+ firstCategory +
                     ", nextCategory= " + nextCategory +")", compCategory >= 0);
        }
        else
        {
          assertTrue("should <=0 (firstCategory= "+ firstCategory +
                     ", nextCategory= "+ nextCategory+")", compCategory <= 0);
        }
      }
    }
  }


  /**
   * @param res
   *          result
   * @param selName
   *          the field name of the facet
   * @param selVal
   *          the value for which to check the count
   * @param count
   *          the expected count of the given value. If count>0, we verify the count. If count=0, it either has to NOT exist or it is 0.
   *          If count <0, it must not exist.
   */
  private void verifyFacetCount(SenseiResult res, String selName, String selVal, int count)
  {
    FacetAccessible year = res.getFacetAccessor(selName);
    List<BrowseFacet> browsefacets = year.getFacets();
    int index = indexOfFacet(selVal, browsefacets);
    if (count>0)
    {
    assertTrue("should contain a BrowseFacet for " + selVal, index >= 0);
    BrowseFacet bf = browsefacets.get(index);
    assertEquals(selVal + " has wrong count ", count, bf.getFacetValueHitCount());
    } else if (count == 0)
    {
      if (index >= 0)
      {
        // count has to be 0
        BrowseFacet bf = browsefacets.get(index);
        assertEquals(selVal + " has wrong count ", count, bf.getFacetValueHitCount());
      }
    } else
    {
      assertTrue("should not contain a BrowseFacet for " + selVal, index < 0);
    }
  }

  private int indexOfFacet(String selVal, List<BrowseFacet> browsefacets)
  {
    for (int i = 0; i < browsefacets.size(); i++)
    {
      if (browsefacets.get(i).getValue().equals(selVal))
        return i;
    }
    return -1;
  }  
  public void testBqlExtraWithRangeTemplateVariables() throws Exception
  {
    logger.info("Executing test case testBqlExtraFilter");
    String req = "{  \"bql\": \"select * FROM sensei WHERE groupid>= $startDate AND groupid<= $endDate ORDER BY groupid ASC limit 0, 500\",   \"templateMapping\": {    \"endDate\": \"1343800000000\",    \"startDate\": \"0\"  }}";
    JSONObject res = search(new JSONObject(req));
    System.out.println("!!!" + res.toString(1));
    assertEquals("numhits is wrong", 14991, res.getInt("numhits"));
  }
  public void testBqlSortAndRangeByActivityColumn() throws Exception
  {
    logger.info("Executing test case testBqlSortAndRangeByActivityColumn");
    String req = "{  \"bql\": \"select * FROM sensei WHERE likes>= 1 ORDER BY likes DESC limit 0, 500\"}";
    JSONObject res = search(new JSONObject(req));
    System.out.println("!!!" + res.toString(1));
    assertTrue(res.getInt("numhits") > 0);
  }
  public void testBqlExtraWithRangeTemplateVariables2() throws Exception
  {
    logger.info("Executing test case testBqlExtraFilter");
    String req = "{  \"bql\": \"select * FROM sensei WHERE groupid >= 1 AND groupid<= 1343700000000 ORDER BY groupid ASC limit 0, 500\"}";
    JSONObject res = search(new JSONObject(req));
    //System.out.println("!!!" + res.toString(1));
    assertEquals("numhits is wrong", 14990, res.getInt("numhits"));
  }
  public void testSelectionDynamicTimeRange() throws Exception
  {
    logger.info("executing test case testSelection");


    SenseiRequest req = new SenseiRequest();
    DefaultFacetHandlerInitializerParam initParam = new DefaultFacetHandlerInitializerParam();
    initParam.putLongParam("time", new long[]{15000L});
    req.setFacetHandlerInitializerParam("timeRange", initParam);
    //req.setFacetHandlerInitializerParam("timeRange_internal", new DefaultFacetHandlerInitializerParam());
    req.setCount(3);
    //setspec(req, facetSpecall);
    BrowseSelection sel = new BrowseSelection("timeRange");
    String selVal = "000000013";
    sel.addValue(selVal);
    req.addSelection(sel);
     SenseiResult res = broker.browse(req);
    logger.info("request:" + req + "\nresult:" + res);
    assertEquals(12990, res.getNumHits());

  }
}
