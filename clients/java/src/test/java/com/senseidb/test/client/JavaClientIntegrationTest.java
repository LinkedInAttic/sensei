package com.senseidb.test.client;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.senseidb.search.client.SenseiServiceProxy;
import com.senseidb.search.client.json.JsonSerializer;
import com.senseidb.search.client.req.Operator;
import com.senseidb.search.client.req.Selection;
import com.senseidb.search.client.req.SenseiClientRequest;
import com.senseidb.search.client.req.Sort;
import com.senseidb.search.client.req.filter.Filter;
import com.senseidb.search.client.req.filter.Filters;
import com.senseidb.search.client.req.query.Queries;
import com.senseidb.search.client.req.query.Query;
import com.senseidb.search.client.req.relevance.Model;
import com.senseidb.search.client.req.relevance.Relevance;
import com.senseidb.search.client.req.relevance.RelevanceFacetType;
import com.senseidb.search.client.req.relevance.RelevanceValues;
import com.senseidb.search.client.req.relevance.VariableType;
import com.senseidb.search.client.res.SenseiResult;
@Ignore
public class JavaClientIntegrationTest extends Assert {
  private SenseiServiceProxy senseiServiceProxy;
  @Before
  public void setUp () {
    senseiServiceProxy = new SenseiServiceProxy("localhost", 8080);
  }
  @Test
  public void testSelectionRange() throws Exception
  {
    //2000 1548;
    //2001 1443;
    //2002 1464;
    // [2000 TO 2002]   ==> 4455
    // (2000 TO 2002)   ==> 1443
    // (2000 TO 2002]   ==> 2907
    // [2000 TO 2002)   ==> 2991
      SenseiClientRequest request = SenseiClientRequest.builder().addSelection(Selection.range("year", "2000", "2002")).build();
      SenseiResult res = senseiServiceProxy.sendSearchRequest(request);
      assertEquals("numhits is wrong", 4455, res.getNumhits().intValue());

      request = SenseiClientRequest.builder().addSelection(Selection.range("year", "2000", "2002", false, false)).build();
      res = senseiServiceProxy.sendSearchRequest( request);
      assertEquals("numhits is wrong", 1443, res.getNumhits().intValue());

      request = SenseiClientRequest.builder().addSelection(Selection.range("year", "2000", "2002", false, true)).build();
      res = senseiServiceProxy.sendSearchRequest( request);
      assertEquals("numhits is wrong", 2907, res.getNumhits().intValue());

      request = SenseiClientRequest.builder().addSelection(Selection.range("year", "2000", "2002", true, false)).build();
      res = senseiServiceProxy.sendSearchRequest( request);
      assertEquals("numhits is wrong", 2991, res.getNumhits().intValue());
  }
  @Test
  public void testMatchAllWithBoostQuery() throws Exception
  {
    SenseiClientRequest request = SenseiClientRequest.builder().query(Queries.matchAllQuery(1.2)).build();
    SenseiResult res = senseiServiceProxy.sendSearchRequest( request);
    assertEquals("numhits is wrong", 15000, res.getNumhits().intValue());
  }
  @Test
  public void testQueryStringQuery() throws Exception
  {
    SenseiClientRequest request = SenseiClientRequest.builder().query(Queries.stringQuery("red AND cool")).build();
    SenseiResult res = senseiServiceProxy.sendSearchRequest( request);
    assertEquals("numhits is wrong", 1070, res.getNumhits().intValue());

  }
 // @Test
  public void testUIDQueryRaw() throws Exception
  {
    String req = "{\"query\": {\"ids\": {\"values\": [\"1\", \"2\", \"3\"], \"excludes\": [\"2\"]}}}";
    System.out.println(req);
    JSONObject res =new JSONObject(senseiServiceProxy.sendPostRaw(senseiServiceProxy.getSearchUrl(), req));
    assertEquals("numhits is wrong", 2, res.getInt("numhits"));
    assertEquals("the first uid is wrong", 1, res.getJSONArray("hits").getJSONObject(0).getInt("uid"));
    assertEquals("the second uid is wrong", 3, res.getJSONArray("hits").getJSONObject(1).getInt("uid"));
  }
  @Test
  public void testUIDQuery() throws Exception
  {
    SenseiClientRequest request = SenseiClientRequest.builder().query(Queries.ids(Arrays.asList("1","2", "3"), Arrays.asList("2"), 1.0)).build();

    SenseiResult res = senseiServiceProxy.sendSearchRequest( request);


    assertEquals("numhits is wrong", 2, res.getNumhits().intValue());
    assertEquals("the first uid is wrong", 1, res.getHits().get(0).getUid().intValue());
    assertEquals("the second uid is wrong", 3, res.getHits().get(1).getUid().intValue());
  }
  @Test
  public void testTextQuery() throws Exception
  {
    SenseiClientRequest request = SenseiClientRequest.builder().query(Queries.textQuery("contents", "red cool", Operator.and, 1.0)).build();
    SenseiResult res = senseiServiceProxy.sendSearchRequest( request);
    assertEquals("numhits is wrong", 1070, res.getNumhits().intValue());

  }
  @Test
  public void testTermQuery() throws Exception
  {
    SenseiClientRequest request = SenseiClientRequest.builder().query(Queries.term("color", "red", 1.0)).build();
    SenseiResult res = senseiServiceProxy.sendSearchRequest( request);
    assertEquals("numhits is wrong", 2160, res.getNumhits().intValue());
  }
  
  @Test
  public void testSimpleBQL() throws Exception{
    String bql = "select *  where color in ('red')";
    SenseiResult res = senseiServiceProxy.sendBQL(bql);
    assertEquals("numhits is wrong", 2160, res.getNumhits().intValue()); 
  }
  
  @Test
  public void testTermsQuery() throws Exception
  {
    SenseiClientRequest request = SenseiClientRequest.builder().query(Queries.terms("tags", Arrays.asList("leather", "moon-roof"), Arrays.asList("hybrid"), Operator.or, 0, 1.0)).build();
    SenseiResult res = senseiServiceProxy.sendSearchRequest( request);
    assertEquals("numhits is wrong", 5777, res.getNumhits().intValue());

  }
  @Test
  public void testBooleanQuery() throws Exception
  {
    SenseiClientRequest request = SenseiClientRequest.builder().query(
        Queries.bool(Arrays.asList((Query)Queries.term("color", "red", 1.0)), Arrays.asList((Query)Queries.term("category", "compact", 1.0)), null,  1.0)).build();
    SenseiResult res = senseiServiceProxy.sendSearchRequest( request);
    assertEquals("numhits is wrong", 1652, res.getNumhits().intValue());

  }
  @Test
  public void testDisMaxQuery() throws Exception
  {
    SenseiClientRequest request = SenseiClientRequest.builder().query(
        Queries.disMax(0.7, 1.2, Queries.term("color", "red", 1.0), Queries.term("color", "blue", 1.0))

   ).build();
    SenseiResult res = senseiServiceProxy.sendSearchRequest( request);
    assertEquals("numhits is wrong", 3264, res.getNumhits().intValue());

  }
  @Test
  public void testPathQuery() throws Exception
  {
    SenseiClientRequest request = SenseiClientRequest.builder().query(
        Queries.path("makemodel","asian/acura/3.2tl" , 1.0)

   ).build();
    SenseiResult res = senseiServiceProxy.sendSearchRequest( request);
    assertEquals("numhits is wrong", 126, res.getNumhits().intValue());

  }
  @Test
  public void testPrefixQuery() throws Exception
  {
    SenseiClientRequest request = SenseiClientRequest.builder().query(
        Queries.prefix("color","blu" , 2.0)

   ).build();
    SenseiResult res = senseiServiceProxy.sendSearchRequest( request);
    assertEquals("numhits is wrong", 1104, res.getNumhits().intValue());

  }
  @Test
  public void testWildcardQuery() throws Exception
  {
    SenseiClientRequest request = SenseiClientRequest.builder().query(
        Queries.wildcard("color","bl*e" , 2.0)

   ).build();
    SenseiResult res = senseiServiceProxy.sendSearchRequest( request);
    assertEquals("numhits is wrong", 1104, res.getNumhits().intValue());

  }
  @Test
  public void testRangeQuery() throws Exception
  {
    SenseiClientRequest request = SenseiClientRequest.builder().query(
        Queries.range("year", "1999", "2000", true, true, 2.0, false)

   ).build();
    SenseiResult res = senseiServiceProxy.sendSearchRequest( request);
    assertEquals("numhits is wrong", 3015, res.getNumhits().intValue());

  }
  @Test
  public void testRangeQuery2() throws Exception
  {
    SenseiClientRequest request = SenseiClientRequest.builder().query(
        Queries.range("year", "1999", "2000", true, true, 2.0, false, "int")

   ).build();
    System.out.println(JsonSerializer.serialize(request));
    SenseiResult res = senseiServiceProxy.sendSearchRequest( request);
    assertEquals("numhits is wrong", 3015, res.getNumhits().intValue());

  }
  @Test
  public void testFilteredQuery() throws Exception
  {
    SenseiClientRequest request = SenseiClientRequest.builder().query(
        Queries.filteredQuery(Queries.term("color", "red", 1.0), Filters.range("year", "1999", "2000"), 1.0)

   ).build();
    System.out.println(JsonSerializer.serialize(request));
    SenseiResult res = senseiServiceProxy.sendSearchRequest( request);
    assertEquals("numhits is wrong", 447, res.getNumhits().intValue());

  }
  @Test
  public void testSpanTermQuery() throws Exception
  {
    SenseiClientRequest request = SenseiClientRequest.builder().query(
        Queries.spanTerm("color", "red", 1.0)

   ).build();
    System.out.println(JsonSerializer.serialize(request));
    SenseiResult res = senseiServiceProxy.sendSearchRequest( request);
    assertEquals("numhits is wrong", 2160, res.getNumhits().intValue());

  }
  @Test
  public void testSpanOrQuery() throws Exception
  {
    SenseiClientRequest request = SenseiClientRequest.builder().query(
        Queries.spanOr(1.0, Queries.spanTerm("color", "red", 1.0), Queries.spanTerm("color", "blue"))

   ).build();
    System.out.println(JsonSerializer.serialize(request));
    SenseiResult res = senseiServiceProxy.sendSearchRequest( request);
    assertEquals("numhits is wrong", 3264, res.getNumhits().intValue());

  }


  public void testSpanOrQueryRaw() throws Exception
  {
    String req = "{\"query\":{\"span_or\":{\"clauses\":[{\"span_term\":{\"color\":\"red\"}},{\"span_term\":{\"color\":\"blue\"}}]}}}";
    System.out.println(req);
    JSONObject res =new JSONObject(senseiServiceProxy.sendPostRaw(senseiServiceProxy.getSearchUrl(), req));
    assertEquals("numhits is wrong", 3264, res.getInt("numhits"));
  }
  @Test
  public void testSpanNotQuery() throws Exception
  {
    SenseiClientRequest request = SenseiClientRequest.builder().query(
        Queries.spanNot( Queries.spanTerm("contents", "compact", 1.0), Queries.spanTerm("contents", "red", 1.0), 1.0)

   ).build();
    System.out.println(JsonSerializer.serialize(request));
    SenseiResult res = senseiServiceProxy.sendSearchRequest( request);
    assertEquals("numhits is wrong", 4596, res.getNumhits().intValue());

  }
  @Test
  public void testSpanNearQuery1() throws Exception
  {
    SenseiClientRequest request = SenseiClientRequest.builder().query(
        Queries.spanNear(Arrays.asList(Queries.spanTerm("contents", "red"), Queries.spanTerm("contents", "compact"), Queries.spanTerm("contents", "hybrid")), 12, false, false, 1.0)

   ).build();
    System.out.println(JsonSerializer.serialize(request));
    SenseiResult res = senseiServiceProxy.sendSearchRequest( request);
    assertEquals("numhits is wrong", 274, res.getNumhits().intValue());

  }
  @Test
  public void testSpanNearQuery2() throws Exception
  {
    SenseiClientRequest request = SenseiClientRequest.builder().query(
        Queries.spanNear(Arrays.asList(Queries.spanTerm("contents", "red"), Queries.spanTerm("contents", "compact"), Queries.spanTerm("contents", "favorite")), 0, true, false, 1.0)

   ).build();
    System.out.println(JsonSerializer.serialize(request));
    SenseiResult res = senseiServiceProxy.sendSearchRequest( request);
    assertEquals("numhits is wrong", 63, res.getNumhits().intValue());

  }

  @Test
  public void testSpanFirstQuery() throws Exception
  {
    SenseiClientRequest request = SenseiClientRequest.builder().query(
       Queries.spanFirst(Queries.spanTerm("color", "red"), 2, 1.0)

   ).build();
    System.out.println(JsonSerializer.serialize(request));
    SenseiResult res = senseiServiceProxy.sendSearchRequest( request);
    assertEquals("numhits is wrong", 2160, res.getNumhits().intValue());

  }
  @Test
  public void testUIDFilter() throws Exception
  {
    SenseiClientRequest request = SenseiClientRequest.builder().filter(Filters.ids(Arrays.asList("1","2", "3"), Arrays.asList("2"))).build();

    SenseiResult res = senseiServiceProxy.sendSearchRequest( request);


    assertEquals("numhits is wrong", 2, res.getNumhits().intValue());
    assertEquals("the first uid is wrong", 1, res.getHits().get(0).getUid().intValue());
    assertEquals("the second uid is wrong", 3, res.getHits().get(1).getUid().intValue());
  }
  @Test
  public void testAndFilter() throws Exception
  {
    SenseiClientRequest request = SenseiClientRequest.builder().filter(
       Filters.and(Filters.term("tags", "mp3") , Filters.term("color", "red"))

   ).build();
    System.out.println(JsonSerializer.serialize(request));
    SenseiResult res = senseiServiceProxy.sendSearchRequest( request);
    assertEquals("numhits is wrong", 439, res.getNumhits().intValue());

  }
  @Test
  public void testOrFilter() throws Exception
  {
    SenseiClientRequest request = SenseiClientRequest.builder().filter(
       Filters.or(Filters.term("color", "blue") , Filters.term("color", "red"))

   ).build();
    System.out.println(JsonSerializer.serialize(request));
    SenseiResult res = senseiServiceProxy.sendSearchRequest( request);
    assertEquals("numhits is wrong", 3264, res.getNumhits().intValue());

  }
  @Test
  public void testBooleanFilter() throws Exception
  {
    SenseiClientRequest request = SenseiClientRequest.builder().filter(
        Filters.bool(Arrays.asList((Filter)Filters.term("color", "red")), Arrays.asList((Filter)Filters.term("category", "compact")), Arrays.asList((Filter)Filters.term("color", "red")))
    ).build();
    SenseiResult res = senseiServiceProxy.sendSearchRequest( request);
    assertEquals("numhits is wrong", 1652, res.getNumhits().intValue());

  }
  @Test
  public void testQueryFilter() throws Exception
  {
    SenseiClientRequest request = SenseiClientRequest.builder().filter(
       Filters.query(Queries.range("year", "1999", "2000",true, true, 1.0, false))

   ).build();
    System.out.println(JsonSerializer.serialize(request));
    SenseiResult res = senseiServiceProxy.sendSearchRequest( request);
    assertEquals("numhits is wrong", 3015, res.getNumhits().intValue());

  }
  @Test
  public void testTermFilter() throws Exception
  {
    SenseiClientRequest request = SenseiClientRequest.builder().filter(
       Filters.term("color", "red")

   ).build();
    System.out.println(JsonSerializer.serialize(request));
    SenseiResult res = senseiServiceProxy.sendSearchRequest( request);
    assertEquals("numhits is wrong", 2160, res.getNumhits().intValue());

  }
  @Test
  public void testSingleField() throws Exception
  {
    SenseiClientRequest request = SenseiClientRequest.builder().showOnlyFields("color")

   .build();
    System.out.println(JsonSerializer.serialize(request));
    SenseiResult res = senseiServiceProxy.sendSearchRequest( request);
    assertEquals( 1, res.getHits().get(0).getFieldValues().size());

  }
  @Test
  public void testTermsFilter() throws Exception
  {
    SenseiClientRequest request = SenseiClientRequest.builder().filter(
       Filters.terms("tags", Arrays.asList("leather", "moon-roof"), Arrays.asList("hybrid"),Operator.or)

   ).build();
    System.out.println(JsonSerializer.serialize(request));
    SenseiResult res = senseiServiceProxy.sendSearchRequest( request);
    assertEquals("numhits is wrong", 5777, res.getNumhits().intValue());

  }
  @Test
  public void testTemplateMapping() throws Exception
  {
    SenseiClientRequest request = SenseiClientRequest.builder().filter(
       Filters.range("year", "$from", "$to")

   )
   .explain(true)
   .addTemplateMapping("from", "1999")
   .addTemplateMapping("to", "2000")
   .build();
    System.out.println(JsonSerializer.serialize(request));
    SenseiResult res = senseiServiceProxy.sendSearchRequest( request);
    //System.out.println(res);
    assertEquals("numhits is wrong", 3015, res.getNumhits().intValue());

  }

  @Test
  public void testGetStoreQuery() throws Exception
  {

      Map<Long, JSONObject> ret = senseiServiceProxy.sendGetRequest(1L,2L, 3L, 5L);

      assertEquals(4, ret.size());
      assertEquals(Integer.valueOf(1), ret.get(1L).get("id"));
      assertEquals(11, ret.get(1L).names().length());
      assertEquals(Integer.valueOf(2), ret.get(2L).get("id"));
      assertEquals(Integer.valueOf(3), ret.get(3L).get("id"));
      assertEquals(Integer.valueOf(5), ret.get(5L).get("id"));
      assertEquals("automatic,hybrid,leather,reliable", ret.get(5L).get("tags"));
  }
  @Test
  public void testRelevance() throws Exception
  {
    Model model = Model.builder().addFacets(RelevanceFacetType.type_int, "year","mileage").
        addFacets(RelevanceFacetType.type_long, "groupid").addFacets(RelevanceFacetType.type_string, "color","category").
        addFunctionParams("_INNER_SCORE", "thisYear", "year","goodYear","mileageWeight","mileage","color", "yearcolor", "colorweight", "category", "categorycolor").
        addVariables(VariableType.set_int, "goodYear").addVariables(VariableType.type_int, "thisYear").
        addVariables(VariableType.map_int_float, "mileageWeight").addVariables(VariableType.map_int_string, "yearcolor")
        .addVariables(VariableType.map_string_float, "colorweight").addVariables(VariableType.map_string_string, "categorycolor").saveAs("model", true).
        function(" if(categorycolor.containsKey(category) && categorycolor.get(category).equals(color))  return 10000f; if(colorweight.containsKey(color) ) return 200f + colorweight.getFloat(color); if(yearcolor.containsKey(year) && yearcolor.get(year).equals(color)) return 200f; if(mileageWeight.containsKey(mileage)) return 10000+mileageWeight.get(mileage); if(goodYear.contains(year)) return (float)Math.exp(2d);   if(year==thisYear) return 87f   ; return  _INNER_SCORE;").build();
    Map<Object, Object> map = new HashMap<Object, Object>();
    map.put("red", 335.5);
    RelevanceValues.RelevanceValuesBuilder valuesBuilder = new RelevanceValues.RelevanceValuesBuilder().addAtomicValue("thisYear", 2001)
        .addListValue("goodYear", 1996,1997).
        addMapValue("mileageWeight", Arrays.asList(11400,11000), Arrays.asList(777.9, 10.2))
        .addMapValue("colorweight", map);
    map.clear();
    map.put(1998, "red");
    valuesBuilder.addMapValue("yearcolor", map);
    valuesBuilder.addMapValue("categorycolor", Arrays.asList("compact"), Arrays.asList("white"));
    SenseiClientRequest request = SenseiClientRequest.builder().addSort(Sort.byRelevance()).query(Queries.stringQuery("").setRelevance(Relevance.valueOf(model, valuesBuilder.build()))).build();
    SenseiResult senseiResult = senseiServiceProxy.sendSearchRequest(request);
    assertEquals(10777, senseiResult.getHits().get(0).getScore().intValue());
    assertEquals(0, senseiResult.getErrorCode().intValue());
  }
  @Test
  public void testError() throws Exception{
    String bql = "select1 *  where color in ('red')";
    SenseiResult res = senseiServiceProxy.sendBQL(bql);
    assertEquals( 150, res.getErrorCode().intValue());
    assertEquals( 1, res.getErrors().size());
    assertEquals( "[line:1, col:0] No viable alternative (token=select1)", res.getErrors().get(0).getMessage());
    assertEquals( "BQLParsingError", res.getErrors().get(0).getErrorType());
  }
  @Test
  public void testMapReduce() throws Exception{
    SenseiResult res = senseiServiceProxy.sendSearchRequest(Examples.mapReduce(SenseiClientRequest.builder()).build());    
    assertEquals("{\"min\":2100,\"uid\":4757}", res.getMapReduceResult().toString());
    
  }
  /* Need to fix the bug in bobo and kamikazi, for details see the following two test cases:*/

//  public void testAndFilter1() throws Exception
//  {
//    logger.info("executing test case testAndFilter1");
//    String req = "{\"filter\":{\"and\":[{\"term\":{\"color\":\"blue\",\"_noOptimize\":false}},{\"query\":{\"term\":{\"category\":\"compact\"}}}]}}";
//    JSONObject res = search(new JSONObject(req));
//    assertEquals("numhits is wrong", 504, res.getInt("numhits"));
//  }
//
//  public void testQueryFilter1() throws Exception
//  {
//    logger.info("executing test case testQueryFilter1");
//    String req = "{\"filter\": {\"query\":{\"term\":{\"category\":\"compact\"}}}}";
//    JSONObject res = search(new JSONObject(req));
//    assertEquals("numhits is wrong", 4169, res.getInt("numhits"));
//  }


  /*  another weird bug may exist somewhere in bobo or kamikazi.*/
  /*  In the following two test cases, when modifying the first one by changing "tags" to "tag", it is supposed that
   *  Only the first test case is not correct, but the second one also throw one NPE, which is weird.
   * */
//  public void testAndFilter2() throws Exception
//  {
//    logger.info("executing test case testAndFilter2");
//    String req = "{\"filter\":{\"and\":[{\"term\":{\"tags\":\"mp3\",\"_noOptimize\":false}},{\"query\":{\"term\":{\"color\":\"red\"}}}]}}";
//    JSONObject res = search(new JSONObject(req));
//    assertEquals("numhits is wrong", 439, res.getInt("numhits"));
//  }
//
//  public void testOrFilter4() throws Exception
//  {
//    //color:blue  ==> 1104
//    //color:red   ==> 2160
//    logger.info("executing test case testOrFilter4");
//    String req = "{\"filter\":{\"or\":[{\"term\":{\"color\":\"blue\",\"_noOptimize\":false}},{\"query\":{\"term\":{\"color\":\"red\"}}}]}}";
//    JSONObject res = search(new JSONObject(req));
//    assertEquals("numhits is wrong", 3264, res.getInt("numhits"));
//  }



}
