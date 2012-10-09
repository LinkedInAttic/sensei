package com.senseidb.test.bql.parsers;

import java.util.Map;
import java.util.HashMap;
import junit.framework.TestCase;

import org.antlr.runtime.*;
import org.junit.Test;

import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONException;

import com.senseidb.bql.parsers.BQLCompiler;
import com.senseidb.util.JsonTemplateProcessor;
import com.senseidb.util.JSONUtil.FastJSONArray;
import com.senseidb.util.JSONUtil.FastJSONObject;

import java.text.ParseException;
import java.text.SimpleDateFormat;

public class TestBQL extends TestCase
{

  private BQLCompiler _compiler;
  private JsonComparator _comp = new JsonComparator(1);

  public TestBQL()
  {
    super();
    Map<String, String[]> facetInfoMap = new HashMap<String, String[]>();
    facetInfoMap.put("tags", new String[]{"multi", "string"});
    facetInfoMap.put("category", new String[]{"simple", "string"});
    facetInfoMap.put("price", new String[]{"range", "float"});
    facetInfoMap.put("mileage", new String[]{"range", "int"});
    facetInfoMap.put("color", new String[]{"simple", "string"});
    facetInfoMap.put("year", new String[]{"range", "int"});
    facetInfoMap.put("makemodel", new String[]{"path", "string"});
    facetInfoMap.put("city", new String[]{"path", "string"});
    facetInfoMap.put("long_id", new String[]{"simple", "long"});
    facetInfoMap.put("groupid_range", new String[]{"range", "long"});
    facetInfoMap.put("time", new String[]{"custom", ""}); // Mimic a custom facet
    _compiler = new BQLCompiler(facetInfoMap);
  }
  @Test
  public void testDateRange() throws Exception
  {
    String bql = "select long_id , city, _grouphitscount FROM ucp WHERE tags IN ('nus:category:profile') AND color IN ('linkedin:profile-update', 'linkedin:profile-add', 'linkedin:profile-become', 'linkedin:profile-be-studying') AND " +
    		"groupid_range >= $startDate AND groupid_range <= $endDate ORDER BY groupid_range ASC limit 0, 500 ROUTE BY \"0\"\n";
    System.out.println(_compiler.compile(bql).toString());
  }
  @Test
  public void testBasic1() throws Exception
  {
    System.out.println("testBasic1");
    System.out.println("==================================================");
    // No where clause
    JSONObject json = _compiler.compile(
      "select category /* BLOCK COMMENTS */ " +
      "from cars       -- LINE COMMENTS"
      );
    JSONObject expected = new JSONObject("{\"meta\":{\"select_list\":[\"category\"]}}");
    assertTrue(_comp.isEquals(json, expected));
  }

  @Test
  public void testBasic2() throws Exception
  {
    System.out.println("testBasic2");
    System.out.println("==================================================");
    // No where clause, with a '*' in SELECT list
    JSONObject json = _compiler.compile(
      "select * " +
      "from cars "
      );
    JSONObject expected = new JSONObject("{\"meta\":{\"select_list\":[\"*\"]}}");
    assertTrue(_comp.isEquals(json, expected));
  }

  @Test
  public void testOrderBy() throws Exception
  {
    System.out.println("testOrderBy");
    System.out.println("==================================================");

    JSONObject json = _compiler.compile(
      "SELECT category " +
      "FROM cars " +
      "ORDER BY color"
      );

    JSONObject expected = new JSONObject("{\"sort\": [{\"color\": \"asc\"}], \"meta\":{\"select_list\":[\"category\"]}}");
    assertTrue(_comp.isEquals(json, expected));
  }

  @Test
  public void testOrderBy2() throws Exception
  {
    System.out.println("testOrderBy2");
    System.out.println("==================================================");

    JSONObject json = _compiler.compile(
      "SELECT category " +
      "FROM cars " +
      "ORDER BY color, price DESC, year ASC"
      );
    JSONObject expected = new JSONObject("{\"sort\": [{\"color\": \"asc\"},{\"price\": \"desc\"},{\"year\": \"asc\"}], \"meta\":{\"select_list\":[\"category\"]}}");
    assertTrue(_comp.isEquals(json, expected));
  }

  @Test
  public void testOrderByRelevance() throws Exception
  {
    System.out.println("testOrderByRelevance");
    System.out.println("==================================================");

    JSONObject json = _compiler.compile(
      "SELECT category " +
      "FROM cars " +
      "ORDER BY relevance"
      );
    JSONObject expected = new JSONObject("{\"sort\":\"relevance\",\"meta\":{\"select_list\":[\"category\"]}}");
    assertTrue(_comp.isEquals(json, expected));
  }

  @Test
  public void testLimit1() throws Exception
  {
    System.out.println("testLimit1");
    System.out.println("==================================================");

    JSONObject json = _compiler.compile(
      "SELECT category " +
      "FROM cars " +
      "LIMIT 123"
      );
    JSONObject expected = new JSONObject("{\"from\": 0, \"size\": 123, \"meta\":{\"select_list\":[\"category\"]}}");
    assertTrue(_comp.isEquals(json, expected));
  }

  @Test
  public void testLimit2() throws Exception
  {
    System.out.println("testLimit2");
    System.out.println("==================================================");

    JSONObject json = _compiler.compile(
      "SELECT category " +
      "FROM cars " +
      "LIMIT 15, 30"
      );
    JSONObject expected = new JSONObject("{\"from\": 15, \"size\": 30, \"meta\":{\"select_list\":[\"category\"]}}");
    assertTrue(_comp.isEquals(json, expected));
  }

  @Test
  public void testGroupBy1() throws Exception
  {
    System.out.println("testGroupBy1");
    System.out.println("==================================================");

    JSONObject json = _compiler.compile(
      "SELECT category " +
      "FROM cars " +
      "GROUP BY color"
      );
    JSONObject expected = new JSONObject("{\"groupBy\": {\"columns\": [\"color\"],\"top\":10}, \"meta\":{\"select_list\":[\"category\"]}}");
    assertTrue(_comp.isEquals(json, expected));
  }

  @Test
  public void testGroupBy2() throws Exception
  {
    System.out.println("testGroupBy2");
    System.out.println("==================================================");

    JSONObject json = _compiler.compile(
      "SELECT category " +
      "FROM cars " +
      "GROUP BY color TOP 5"
      );
    JSONObject expected = new JSONObject("{\"groupBy\": {\"columns\": [\"color\"], \"top\":5}, \"meta\":{\"select_list\":[\"category\"]}}");
    assertTrue(_comp.isEquals(json, expected));
  }

  @Test
  public void testGroupByOrColumns() throws Exception
  {
    System.out.println("testGroupByOrColumns");
    System.out.println("==================================================");

    JSONObject json = _compiler.compile(
      "SELECT category " +
      "FROM cars " +
      "GROUP BY color OR category TOP 5"
      );
    JSONObject expected = new JSONObject("{\"groupBy\":{\"columns\":[\"color\",\"category\"],\"top\":5},\"meta\":{\"select_list\":[\"category\"]}}");
    assertTrue(_comp.isEquals(json, expected));
  }

  @Test
  public void testEqualPredInteger() throws Exception
  {
    System.out.println("testEqualPredInteger");
    System.out.println("==================================================");

    JSONObject json = _compiler.compile(
      "SELECT category " +
      "FROM cars " +
      "WHERE year = 1999"
      );
    JSONObject expected = new JSONObject("{\"selections\":[{\"range\":{\"year\":{\"to\":1999,\"include_lower\":true,\"include_upper\":true,\"from\":1999}}}], \"meta\":{\"select_list\":[\"category\"]}}");
    assertTrue(_comp.isEquals(json, expected));
  }

  @Test
  public void testEqualPredFloat() throws Exception
  {
    System.out.println("testEqualPredFloat");
    System.out.println("==================================================");

    JSONObject json = _compiler.compile(
      "SELECT category " +
      "FROM cars " +
      "WHERE price = 1500.99"
      );
    JSONObject expected = new JSONObject("{\"selections\":[{\"range\":{\"price\":{\"to\":1500.99,\"include_lower\":true,\"include_upper\":true,\"from\":1500.99}}}], \"meta\":{\"select_list\":[\"category\"]}}");
    assertTrue(_comp.isEquals(json, expected));
  }

  @Test
  public void testEqualPredString() throws Exception
  {
    System.out.println("testEqualPredString");
    System.out.println("==================================================");

    JSONObject json = _compiler.compile(
      "SELECT category " +
      "FROM cars " +
      "WHERE color = 'red'"
      );
    JSONObject expected = new JSONObject("{\"selections\": [{\"term\": {\"color\": {\"value\": \"red\"}}}], \"meta\":{\"select_list\":[\"category\"]}}");
    assertTrue(_comp.isEquals(json, expected));
  }

  @Test
  public void testInPred1() throws Exception
  {
    System.out.println("testInPred1");
    System.out.println("==================================================");

    JSONObject json = _compiler.compile(
      "SELECT category " +
      "FROM cars " +
      "WHERE color IN ('red', 'blue', 'yellow')"
      );
    JSONObject expected = new JSONObject("{\"selections\":[{\"terms\":{\"color\":{\"values\":[\"red\",\"blue\",\"yellow\"],\"excludes\":[],\"operator\":\"or\"}}}], \"meta\":{\"select_list\":[\"category\"]}}");
    assertTrue(_comp.isEquals(json, expected));
  }

  @Test
  public void testInPred2() throws Exception
  {
    System.out.println("testInPred2");
    System.out.println("==================================================");

    JSONObject json = _compiler.compile(
      "SELECT category " +
      "FROM cars " +
      "WHERE nonfacet IN ('red')"
      );
    JSONObject expected = new JSONObject("{\"filter\":{\"terms\":{\"nonfacet\":{\"values\":[\"red\"],\"excludes\":[],\"operator\":\"or\",\"_noOptimize\":true}}}, \"meta\":{\"select_list\":[\"category\"]}}");
    assertTrue(_comp.isEquals(json, expected));
  }

  @Test
  public void testInPred3() throws Exception
  {
    System.out.println("testInPred3");
    System.out.println("==================================================");

    int result = 0;
    try
    {
      JSONObject json = _compiler.compile(
        "SELECT category \n" +
        "FROM cars \n" +
        "WHERE year IN (1995,2000) " +
        "  AND color = 'red'"
        );
    }
    catch (RecognitionException err)
    {
      // System.out.println(">>> hit FailedPredicateException = " + err);
      // System.out.println(">>> err.line = " + err.line);
      // System.out.println(">>> err.charPositionInLine = " + err.charPositionInLine);
      // System.out.println(">>> err.token = " + err.token.getText());
      result = 1;
    }
    assertEquals(result, 1);
  }

  @Test
  public void testNotInPred() throws Exception
  {
    System.out.println("testNotInPred");
    System.out.println("==================================================");


    JSONObject json = _compiler.compile(
      "SELECT category " +
      "FROM cars " +
      "WHERE color NOT IN ('red', 'blue', 'yellow') EXCEPT ('black', 'green')"
      );
    JSONObject expected = new JSONObject("{\"selections\":[{\"terms\":{\"color\":{\"excludes\":[\"red\",\"blue\",\"yellow\"],\"values\":[\"black\", \"green\"],\"operator\":\"or\"}}}], \"meta\":{\"select_list\":[\"category\"]}}");
    assertTrue(_comp.isEquals(json, expected));
  }

  @Test
  public void testContainsAll() throws Exception
  {
    System.out.println("testContainsAll");
    System.out.println("==================================================");

    JSONObject json = _compiler.compile(
      "SELECT category " +
      "FROM cars " +
      "WHERE color CONTAINS ALL ('red', 'blue', 'yellow') EXCEPT ('black', 'green')"
      );
    JSONObject expected = new JSONObject("{\"selections\":[{\"terms\":{\"color\":{\"values\":[\"red\",\"blue\",\"yellow\"],\"excludes\":[\"black\", \"green\"],\"operator\":\"and\"}}}], \"meta\":{\"select_list\":[\"category\"]}}");
    assertTrue(_comp.isEquals(json, expected));
  }

  @Test
  public void testPathPred1() throws Exception
  {
    System.out.println("testPathPred1");
    System.out.println("==================================================");

    JSONObject json = _compiler.compile(
      "SELECT * " +
      "FROM cars " +
      "WHERE city = 'china/hongkong' WITH ('strict':false, 'depth':1)"
      );
    JSONObject expected = new JSONObject("{\"selections\":[{\"path\":{\"city\":{\"value\":\"china/hongkong\",\"strict\":false,\"depth\":1}}}], \"meta\":{\"select_list\":[\"*\"]}}");
    assertTrue(_comp.isEquals(json, expected));
  }

  @Test
  public void testPathPred2() throws Exception
  {
    System.out.println("testPathPred2");
    System.out.println("==================================================");

    int result = 0;
    try
    {
      JSONObject json = _compiler.compile(
        "SELECT * " +
        "FROM cars " +
        "WHERE city = 'china/hongkong' WITH ('strict':false, 'ddd':1)"
        );
    }
    catch (RecognitionException err) 
    {
      result = 1;
    }
    assertEquals(result, 1);
  }

  @Test
  public void testNotEqualPred() throws Exception
  {
    System.out.println("testNotEqualPred");
    System.out.println("==================================================");

    JSONObject json = _compiler.compile(
      "SELECT category " +
      "FROM cars " +
      "WHERE color <> 'red'"
      );
    JSONObject expected = new JSONObject("{\"selections\":[{\"terms\":{\"color\":{\"values\":[],\"excludes\":[\"red\"],\"operator\":\"or\"}}}], \"meta\":{\"select_list\":[\"category\"]}}");
    assertTrue(_comp.isEquals(json, expected));
  }

  @Test
  public void testNotEqualForRange() throws Exception
  {
    System.out.println("testNotEqualForRange");
    System.out.println("==================================================");

    long now = System.currentTimeMillis();

    JSONObject json = _compiler.compile(
      "SELECT * " +
      "FROM cars " +
      "WHERE year <> 2000"
      );
    JSONObject expected = new JSONObject("{\"filter\":{\"or\":[{\"range\":{\"year\":{\"to\":2000,\"include_upper\":false}}},{\"range\":{\"year\":{\"include_lower\":false,\"from\":2000}}}]},\"meta\":{\"select_list\":[\"*\"]}}");
    assertTrue(_comp.isEquals(json, expected));
  }

  @Test
  public void testQueryIs() throws Exception
  {
    System.out.println("testQueryIs");
    System.out.println("==================================================");

    JSONObject json = _compiler.compile(
      "SELECT category " +
      "FROM cars " +
      "WHERE QUERY IS 'cool AND moon-roof'"
      );
    JSONObject expected = new JSONObject("{\"query\":{\"query_string\":{\"query\":\"cool AND moon-roof\"}}, \"meta\":{\"select_list\":[\"category\"]}}");
    assertTrue(_comp.isEquals(json, expected));
  }

  @Test
  public void testQueryAndSelection1() throws Exception
  {
    System.out.println("testQueryAndSelection1");
    System.out.println("==================================================");

    JSONObject json = _compiler.compile(
      "SELECT category " +
      "FROM cars " +
      "WHERE QUERY IS 'cool AND moon-roof' " +
      "AND color = 'red'                                -- LINE COMMENTS\n" +
      "AND category = 'sedan'"
      );
    JSONObject expected = new JSONObject("{\"query\":{\"query_string\":{\"query\":\"cool AND moon-roof\"}},\"selections\":[{\"term\":{\"color\":{\"value\":\"red\"}}},{\"term\":{\"category\":{\"value\":\"sedan\"}}}], \"meta\":{\"select_list\":[\"category\"]}}");
    assertTrue(_comp.isEquals(json, expected));
  }

  @Test
  public void testQueryAndSelection2() throws Exception
  {
    System.out.println("testQueryAndSelection2");
    System.out.println("==================================================");

    JSONObject json = _compiler.compile(
      "SELECT category " +
      "FROM cars " +
      "WHERE QUERY IS 'cool AND moon-roof' " +
      "AND age = 12 "
      );
    JSONObject expected = new JSONObject("{\"query\":{\"query_string\":{\"query\":\"cool AND moon-roof\"}},\"filter\":{\"term\":{\"age\":{\"value\":12,\"_noOptimize\":true}}}, \"meta\":{\"select_list\":[\"category\"]}}");
    assertTrue(_comp.isEquals(json, expected));
  }

  @Test
  public void testBrowseBy1() throws Exception
  {
    System.out.println("testBrowseBy1");
    System.out.println("==================================================");

    JSONObject json = _compiler.compile(
      "SELECT category " +
      "FROM cars " +
      "BROWSE BY color"
      );
    JSONObject expected = new JSONObject("{\"facets\":{\"color\":{\"max\":10,\"order\":\"hits\",\"expand\":false,\"minhit\":1}}, \"meta\":{\"select_list\":[\"category\"]}}");
    assertTrue(_comp.isEquals(json, expected));
  }

  @Test
  public void testBrowseBy2() throws Exception
  {
    System.out.println("testBrowseBy2");
    System.out.println("==================================================");

    JSONObject json = _compiler.compile(
      "SELECT category " +
      "FROM cars " +
      "BROWSE BY color, price(true, 1, 20, value), year"
      );
    JSONObject expected = new JSONObject("{\"facets\":{\"price\":{\"max\":20,\"order\":\"val\",\"expand\":true,\"minhit\":1},\"color\":{\"max\":10,\"order\":\"hits\",\"expand\":false,\"minhit\":1},\"year\":{\"max\":10,\"order\":\"hits\",\"expand\":false,\"minhit\":1}}, \"meta\":{\"select_list\":[\"category\"]}}");
    assertTrue(_comp.isEquals(json, expected));
  }

  @Test
  public void testBetweenPred() throws Exception
  {
    System.out.println("testBetweenPred");
    System.out.println("==================================================");

    JSONObject json = _compiler.compile(
      "SELECT category " +
      "FROM cars " +
      "WHERE year BETWEEN 2000 AND 2001"
      );
    JSONObject expected = new JSONObject("{\"selections\":[{\"range\":{\"year\":{\"to\":2001,\"include_lower\":true,\"include_upper\":true,\"from\":2000}}}], \"meta\":{\"select_list\":[\"category\"]}}");
    assertTrue(_comp.isEquals(json, expected));
  }

  @Test
  public void testFetchingStored1() throws Exception
  {
    System.out.println("testFetchingStored1");
    System.out.println("==================================================");

    JSONObject json = _compiler.compile(
      "SELECT * " +
      "FROM cars " +
      "FETCHING STORED FALSE"
      );
    JSONObject expected = new JSONObject("{\"meta\":{\"select_list\":[\"*\"]}}");
    assertTrue(_comp.isEquals(json, expected));
  }

  @Test
  public void testFetchingStored2() throws Exception
  {
    System.out.println("testFetchingStored2");
    System.out.println("==================================================");

    JSONObject json = _compiler.compile(
      "SELECT * " +
      "FROM cars " +
      "FETCHING STORED true"
      );
    JSONObject expected = new JSONObject("{\"fetchStored\":true, \"meta\":{\"select_list\":[\"*\"]}}");
    assertTrue(_comp.isEquals(json, expected));
  }

  @Test
  public void testNotBetweenPred() throws Exception
  {
    System.out.println("testNotBetweenPred");
    System.out.println("==================================================");

    JSONObject json = _compiler.compile(
      "SELECT category " +
      "FROM cars " +
      "WHERE year NOT BETWEEN 2000 AND 2002"
      );
    JSONObject expected = new JSONObject("{\"filter\":{\"or\":[{\"range\":{\"year\":{\"to\":2000,\"include_upper\":false}}},{\"range\":{\"year\":{\"include_lower\":false,\"from\":2002}}}]}, \"meta\":{\"select_list\":[\"category\"]}}");
    assertTrue(_comp.isEquals(json, expected));
  }

  @Test
  public void testRangePred1() throws Exception
  {
    System.out.println("testRangePred1");
    System.out.println("==================================================");

    JSONObject json = _compiler.compile(
      "SELECT year " +
      "FROM cars " +
      "WHERE year > 1999"
      );
    JSONObject expected = new JSONObject("{\"selections\":[{\"range\":{\"year\":{\"from\":1999,\"include_lower\":false}}}], \"meta\":{\"select_list\":[\"year\"]}}");
    assertTrue(_comp.isEquals(json, expected));
  }

  @Test
  public void testRangePred2() throws Exception
  {
    System.out.println("testRangePred2");
    System.out.println("==================================================");

    JSONObject json = _compiler.compile(
      "SELECT year " +
      "FROM cars " +
      "WHERE year <= 2000"
      );
    JSONObject expected = new JSONObject("{\"selections\":[{\"range\":{\"year\":{\"to\":2000,\"include_upper\":true}}}], \"meta\":{\"select_list\":[\"year\"]}}");
    assertTrue(_comp.isEquals(json, expected));
  }

  @Test
  public void testRangePred3() throws Exception
  {
    System.out.println("testRangePred3");
    System.out.println("==================================================");

    JSONObject json = _compiler.compile(
      "SELECT year " +
      "FROM cars " +
      "WHERE year > 1999 AND year <= 2003 AND year >= 1999"
      );
    JSONObject expected = new JSONObject("{\"selections\":[{\"range\":{\"year\":{\"to\":2003,\"include_lower\":false,\"include_upper\":true,\"from\":1999}}}], \"meta\":{\"select_list\":[\"year\"]}}");
    assertTrue(_comp.isEquals(json, expected));
  }

  @Test
  public void testRangePred4() throws Exception
  {
    System.out.println("testRangePred4");
    System.out.println("==================================================");

    JSONObject json = _compiler.compile(
      "SELECT * " +
      "FROM cars " +
      "WHERE name > 'abc' AND name < 'xyz' AND name >= 'ddd'"
      );
    JSONObject expected = new JSONObject("{\"filter\":{\"range\":{\"name\":{\"to\":\"xyz\",\"include_lower\":true,\"include_upper\":false,\"from\":\"ddd\"}}}, \"meta\":{\"select_list\":[\"*\"]}}");
    assertTrue(_comp.isEquals(json, expected));
  }

  @Test
  public void testRangePred5() throws Exception
  {
    System.out.println("testRangePred5");
    System.out.println("==================================================");

    int result = 0;
    try
    {
    JSONObject json = _compiler.compile(
      "SELECT * " +
      "FROM cars " +
      "WHERE year > 1999 AND year < 1995"
      );
    }
    catch (RecognitionException err) 
    {
      result = 1;
    }
    assertEquals(result, 1);
  }

  @Test
  public void testOrPred() throws Exception
  {
    System.out.println("testOrPred");
    System.out.println("==================================================");

    JSONObject json = _compiler.compile(
      "SELECT color " +
      "FROM cars " +
      "WHERE color = 'red' OR year > 1995"
      );
    JSONObject expected = new JSONObject("{\"filter\":{\"or\":[{\"term\":{\"color\":{\"value\":\"red\"}}},{\"range\":{\"year\":{\"include_lower\":false,\"from\":1995}}}]}, \"meta\":{\"select_list\":[\"color\"]}}");
    assertTrue(_comp.isEquals(json, expected));
  }

  @Test
  public void testAndPred() throws Exception
  {
    System.out.println("testAndPred");
    System.out.println("==================================================");

    JSONObject json = _compiler.compile(
      "SELECT color " +
      "FROM cars " +
      "WHERE color = 'red' AND year > 1995"
      );
    JSONObject expected = new JSONObject("{\"selections\":[{\"term\":{\"color\":{\"value\":\"red\"}}},{\"range\":{\"year\":{\"include_lower\":false,\"from\":1995}}}], \"meta\":{\"select_list\":[\"color\"]}}");
    assertTrue(_comp.isEquals(json, expected));
  }

  @Test
  public void testAndOrPred() throws Exception
  {
    System.out.println("testAndOrPred");
    System.out.println("==================================================");

    JSONObject json = _compiler.compile(
      "SELECT color " +
      "FROM cars " +
      "WHERE (color = 'red' OR color = 'blue') " +
      "   OR (color = 'black' AND tags CONTAINS ALL ('hybrid', 'moon-roof', 'leather'))"
      );
    JSONObject expected = new JSONObject("{\"filter\":{\"or\":[{\"or\":[{\"term\":{\"color\":{\"value\":\"red\"}}},{\"term\":{\"color\":{\"value\":\"blue\"}}}]},{\"and\":[{\"term\":{\"color\":{\"value\":\"black\"}}},{\"terms\":{\"tags\":{\"values\":[\"hybrid\",\"moon-roof\",\"leather\"],\"excludes\":[],\"operator\":\"and\"}}}]}]}, \"meta\":{\"select_list\":[\"color\"]}}");
    assertTrue(_comp.isEquals(json, expected));
  }

  @Test
  public void testSelectionAndFilter() throws Exception
  {
    System.out.println("testSelectionAndFilter");
    System.out.println("==================================================");

    // Here "age" is not a facet, so we have to treat it as a filter
    JSONObject json = _compiler.compile(
      "SELECT color " +
      "FROM cars " +
      "WHERE color = 'red' AND age > 25"
      );
    JSONObject expected = new JSONObject("{\"selections\":[{\"term\":{\"color\":{\"value\":\"red\"}}}],\"filter\":{\"range\":{\"age\":{\"include_lower\":false,\"from\":25}}}, \"meta\":{\"select_list\":[\"color\"]}}");
    assertTrue(_comp.isEquals(json, expected));
  }

  @Test
  public void testMultipleQueries() throws Exception
  {
    System.out.println("testMultipleQueries");
    System.out.println("==================================================");

    JSONObject json = _compiler.compile(
      "SELECT color " +
      "FROM cars " +
      "WHERE (color = 'red' AND query is 'hybrid AND cool') " +
      "   OR (color = 'blue' AND query is 'moon-roof AND navigation')"
      );
    JSONObject expected = new JSONObject("{\"filter\":{\"or\":[{\"and\":[{\"term\":{\"color\":{\"value\":\"red\"}}},{\"query\":{\"query_string\":{\"query\":\"hybrid AND cool\"}}}]},{\"and\":[{\"term\":{\"color\":{\"value\":\"blue\"}}},{\"query\":{\"query_string\":{\"query\":\"moon-roof AND navigation\"}}}]}]}, \"meta\":{\"select_list\":[\"color\"]}}");
    assertTrue(_comp.isEquals(json, expected));
  }

  @Test
  public void testMatchPred() throws Exception
  {
    System.out.println("testMatchPred");
    System.out.println("==================================================");

    JSONObject json = _compiler.compile(
      "SELECT color " +
      "FROM cars " +
      "WHERE MATCH(f1, f2) AGAINST('text1 AND text2')"
      );
    JSONObject expected = new JSONObject("{\"query\":{\"query_string\":{\"query\":\"text1 AND text2\",\"fields\":[\"f1\",\"f2\"]}}, \"meta\":{\"select_list\":[\"color\"]}}");
    assertTrue(_comp.isEquals(json, expected));
  }

  @Test
  public void testNotMatchPred() throws Exception
  {
    System.out.println("testNotMatchPred");
    System.out.println("==================================================");

    JSONObject json = _compiler.compile(
      "SELECT color " +
      "FROM cars " +
      "WHERE NOT MATCH(color) AGAINST('red')"
      );
    JSONObject expected = new JSONObject("{\"filter\":{\"bool\":{\"must_not\":{\"query\":{\"query_string\":{\"query\":\"red\",\"fields\":[\"color\"]}}}}},\"meta\":{\"select_list\":[\"color\"]}}");
    assertTrue(_comp.isEquals(json, expected));
  }

  @Test
  public void testLikePredicate1() throws Exception
  {
    System.out.println("testLikePredicate1");
    System.out.println("==================================================");

    JSONObject json = _compiler.compile(
      "SELECT * " +
      "FROM cars " +
      "WHERE category LIKE 's_d%'"
      );
    JSONObject expected = new JSONObject("{\"query\":{\"wildcard\":{\"category\":\"s?d*\"}},\"meta\":{\"select_list\":[\"*\"]}}");
    assertTrue(_comp.isEquals(json, expected));
  }

  @Test
  public void testLikePredicate2() throws Exception
  {
    System.out.println("testLikePredicate2");
    System.out.println("==================================================");

    JSONObject json = _compiler.compile(
      "SELECT * " +
      "FROM cars " +
      "WHERE category LIKE 'sed*'"
      );
    JSONObject expected = new JSONObject("{\"query\":{\"wildcard\":{\"category\":\"sed*\"}},\"meta\":{\"select_list\":[\"*\"]}}");
    assertTrue(_comp.isEquals(json, expected));
  }

  @Test
  public void testLikePredicate3() throws Exception
  {
    System.out.println("testLikePredicate3");
    System.out.println("==================================================");

    int result = 0;
    try 
    {
      JSONObject json = _compiler.compile(
        "SELECT * " +
        "FROM cars " +
        "WHERE price LIKE '123%'"
        );
    }
    catch (RecognitionException err)
    {
      result = 1;
    }
    assertEquals(result, 1);
  }

  @Test
  public void testNotLikePredicate() throws Exception
  {
    System.out.println("testNotLikePredicate");
    System.out.println("==================================================");

    JSONObject json = _compiler.compile(
      "SELECT * " +
      "FROM cars " +
      "WHERE color NOT LIKE 'bl%'"
      );
    JSONObject expected = new JSONObject("{\"filter\":{\"bool\":{\"must_not\":{\"query\":{\"wildcard\":{\"color\":\"bl*\"}}}}},\"meta\":{\"select_list\":[\"*\"]}}");
    assertTrue(_comp.isEquals(json, expected));
  }

  @Test
  public void testQueryAndLike() throws Exception
  {
    System.out.println("testQueryAndLike");
    System.out.println("==================================================");

    JSONObject json = _compiler.compile(
      "SELECT color, category, tags " +
      "FROM cars " +
      "WHERE color LIKE 'bl%' " +
      "  AND MATCH(contents) AGAINST('cool AND moon-roof') " +
      "  AND category LIKE '%an'"
      );
    JSONObject expected = new JSONObject("{\"query\":{\"wildcard\":{\"color\":\"bl*\"}},\"filter\":{\"and\":[{\"query\":{\"query_string\":{\"query\":\"cool AND moon-roof\",\"fields\":[\"contents\"]}}},{\"query\":{\"wildcard\":{\"category\":\"*an\"}}}]},\"meta\":{\"select_list\":[\"color\",\"category\",\"tags\"]}}");
    assertTrue(_comp.isEquals(json, expected));
  }

  @Test
  public void testColumnType1() throws Exception
  {
    System.out.println("testColumnType1");
    System.out.println("==================================================");

    int result = 0;
    try 
    {
      JSONObject json = _compiler.compile(
        "SELECT * " +
        "FROM cars " +
        "WHERE color = 1"
      );
    }
    catch (RecognitionException err) 
    {
      result = 1;
    }
    assertEquals(result, 1);
  }

  @Test
  public void testColumnType2() throws Exception
  {
    System.out.println("testColumnType2");
    System.out.println("==================================================");

    int result = 0;
    try 
    {
      JSONObject json = _compiler.compile(
        "SELECT * " +
        "FROM cars " +
        "WHERE mileage = 111 " +
        "  OR (color IN ('red', 'blue') AND year > 'bbb')"
      );
    }
    catch (RecognitionException err) 
    {
      result = 1;
    }
    assertEquals(result, 1);
  }

  @Test
  public void testColumnType3() throws Exception
  {
    System.out.println("testColumnType3");
    System.out.println("==================================================");

    int result = 0;
    try 
    {
      JSONObject json = _compiler.compile(
        "SELECT * " +
        "FROM cars " +
        "WHERE color IN ('red', 123)"
      );
    }
    catch (RecognitionException err) 
    {
      result = 1;
    }
    assertEquals(result, 1);
  }

  @Test
  public void testColumnType4() throws Exception
  {
    System.out.println("testColumnType4");
    System.out.println("==================================================");

    int result = 0;
    try 
    {
      JSONObject json = _compiler.compile(
        "SELECT * " +
        "FROM cars " +
        "WHERE tags CONTAINS ALL ('cool', 123)"
      );
    }
    catch (RecognitionException err) 
    {
      result = 1;
    }
    assertEquals(result, 1);
  }

  @Test
  public void testGivenClause1() throws Exception
  {
    System.out.println("testGivenClause1");
    System.out.println("==================================================");

    JSONObject json = _compiler.compile(
      "SELECT * " +
      "FROM cars " +
      "GIVEN FACET PARAM (My-Network, 'srcid', int, 8233570)"
      );
    JSONObject expected = new JSONObject("{\"facetInit\":{\"My-Network\":{\"srcid\":{\"values\":[8233570],\"type\":\"int\"}}}, \"meta\":{\"select_list\":[\"*\"]}}");
    assertTrue(_comp.isEquals(json, expected));
  }

  @Test
  public void testGivenClause2() throws Exception
  {
    System.out.println("testGivenClause2");
    System.out.println("==================================================");

    JSONObject json = _compiler.compile(
      "SELECT * " +
      "FROM cars " +
      "GIVEN FACET PARAM (My-Network, 'srcid', int, 8233570), " +
      "                  (time, 'now', long, '999999'), " +
      "                  (member, 'last_name', string, 'Cui'), " + 
      "                  (member, 'age', int, 25)"
      );
    JSONObject expected = new JSONObject("{\"facetInit\":{\"member\":{\"age\":{\"values\":[25],\"type\":\"int\"},\"last_name\":{\"values\":[\"Cui\"],\"type\":\"string\"}},\"time\":{\"now\":{\"values\":[\"999999\"],\"type\":\"long\"}},\"My-Network\":{\"srcid\":{\"values\":[8233570],\"type\":\"int\"}}}, \"meta\":{\"select_list\":[\"*\"]}}");
    assertTrue(_comp.isEquals(json, expected));
  }

  @Test
  public void testGivenClause3() throws Exception
  {
    System.out.println("testGivenClause3");
    System.out.println("==================================================");

    JSONObject json = _compiler.compile(
      "SELECT * " +
      "FROM cars " +
      "GIVEN FACET PARAM (member, 'age', int, (25, 30, 35, 40))"
      );
    JSONObject expected = new JSONObject("{\"facetInit\":{\"member\":{\"age\":{\"values\":[25,30,35,40],\"type\":\"int\"}}},\"meta\":{\"select_list\":[\"*\"]}}");
    assertTrue(_comp.isEquals(json, expected));
  }

  @Test
  public void testGivenClauseVariable() throws Exception
  {
    System.out.println("testGivenClauseVariable");
    System.out.println("==================================================");

    JSONObject json = _compiler.compile(
      "SELECT * " +
      "FROM cars " +
      "GIVEN FACET PARAM (member, 'age', int, $user_age)"
      );

    JSONObject expected = new JSONObject("{\"facetInit\":{\"member\":{\"age\":{\"values\":\"$user_age\",\"type\":\"int\"}}},\"meta\":{\"select_list\":[\"*\"],\"variables\":[\"user_age\"]}}");
    assertTrue(_comp.isEquals(json, expected));

    JsonTemplateProcessor jsonProc = new JsonTemplateProcessor();
    json.put(JsonTemplateProcessor.TEMPLATE_MAPPING_PARAM,
             new FastJSONObject().put("user_age",
                                  new FastJSONArray().put(25)));
    JSONObject newJson = jsonProc.substituteTemplates(json);
    JSONObject expected2 = new JSONObject("{\"facetInit\":{\"member\":{\"age\":{\"values\":[25],\"type\":\"int\"}}},\"templateMapping\":{\"user_age\":[25]},\"meta\":{\"select_list\":[\"*\"],\"variables\":[\"user_age\"]}}");
    assertTrue(_comp.isEquals(newJson, expected2));
  }

  @Test
  public void testTimePred1() throws Exception
  {
    System.out.println("testTimePred1");
    System.out.println("==================================================");

    long now = System.currentTimeMillis();

    JSONObject json = _compiler.compile(
      "SELECT * " +
      "FROM cars " +
      "WHERE time IN LAST 1 weeks 2 day 3 hours 4 mins 5 seconds 6 msecs"
      );

    //System.out.println(">>> json: " + json);

    long timeStamp = Long.parseLong(json.getJSONObject("filter")
      .getJSONObject("range").getJSONObject("time").getString("from"));
    long timeSpan = 1 * (7 * 24 * 60 * 60 * 1000L) +
                    2 * (24 * 60 * 60 * 1000L) +
                    3 * (60 * 60 * 1000L) +
                    4 * (60 * 1000L) +
                    5 * 1000L +
                    6;

    // System.out.println(">>> now - timeStamp = " + (now - timeStamp));
    // System.out.println(">>> timeSpan = " + timeSpan);
    // System.out.println(">>> now - timeStamp - timeSpan = " + (now - timeStamp - timeSpan));
    assertTrue(now - timeStamp - timeSpan < 2); // Should be less than 2 msecs
  }

  @Test
  public void testTimePred2() throws Exception
  {
    System.out.println("testTimePred2");
    System.out.println("==================================================");

    long now = System.currentTimeMillis();

    JSONObject json = _compiler.compile(
      "SELECT * " +
      "FROM cars " +
      "WHERE time SINCE 2 days 3 hours 4 minutes 6 milliseconds AGO"
      );

    long timeStamp = Long.parseLong(json.getJSONObject("filter")
      .getJSONObject("range").getJSONObject("time").getString("from"));
    long timeSpan = 2 * (24 * 60 * 60 * 1000L) +
                    3 * (60 * 60 * 1000L) +
                    4 * (60 * 1000L) +
                    6;
    assertTrue(now - timeStamp - timeSpan < 2); // Should be less than 2 msecs
  }

  @Test
  public void testTimePred3() throws Exception
  {
    System.out.println("testTimePred3");
    System.out.println("==================================================");

    long now = System.currentTimeMillis();

    JSONObject json = _compiler.compile(
      "SELECT * " +
      "FROM cars " +
      "WHERE time BEFORE 3 hours 4 min AGO"
      );

    long timeStamp = Long.parseLong(json.getJSONObject("filter")
      .getJSONObject("range").getJSONObject("time").getString("to"));
    long timeSpan = 3 * (60 * 60 * 1000L) +
                    4 * (60 * 1000L);
    assertTrue(now - timeStamp - timeSpan < 2); // Should be less than 2 msecs
  }

  @Test
  public void testNotTimePred1() throws Exception
  {
    System.out.println("testNotTimePred1");
    System.out.println("==================================================");

    long now = System.currentTimeMillis();

    JSONObject json = _compiler.compile(
      "SELECT * " +
      "FROM cars " +
      "WHERE time NOT BEFORE 3 hours 4 min AGO"
      );

    long timeStamp = Long.parseLong(json.getJSONObject("filter")
      .getJSONObject("range").getJSONObject("time").getString("from"));
    long timeSpan = 3 * (60 * 60 * 1000L) +
                    4 * (60 * 1000L);
    assertTrue(now - timeStamp - timeSpan < 2); // Should be less than 2 msecs
  }

  @Test
  public void testDateTime1() throws Exception
  {
    System.out.println("testDateTime1");
    System.out.println("==================================================");

    long now = System.currentTimeMillis();

    JSONObject json = _compiler.compile(
      "SELECT * " +
      "FROM cars " +
      "WHERE time < 2012-01-02 12:10:30"
      );

    long timeStamp = Long.parseLong(json.getJSONObject("filter")
      .getJSONObject("range").getJSONObject("time").getString("to"));
    long expected = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse("2012-01-02 12:10:30").getTime();
    assertEquals(timeStamp, expected);
  }

  @Test
  public void testDateTime2() throws Exception
  {
    System.out.println("testDateTime2");
    System.out.println("==================================================");

    long now = System.currentTimeMillis();

    JSONObject json = _compiler.compile(
      "SELECT * " +
      "FROM cars " +
      "WHERE time < 2012-01-02"
      );

    long timeStamp = Long.parseLong(json.getJSONObject("filter")
      .getJSONObject("range").getJSONObject("time").getString("to"));
    long expected = new SimpleDateFormat("yyyy-MM-dd").parse("2012-01-02").getTime();
    assertEquals(timeStamp, expected);
  }
  
  @Test
  public void testDateTime3() throws Exception
  {
    System.out.println("testDateTime3");
    System.out.println("==================================================");

    long now = System.currentTimeMillis();

    JSONObject json = _compiler.compile(
      "SELECT * \n" +
      "FROM cars \n" +
      "WHERE time > 2012-01-02 AND time <= 2012/01/31 \n" +
      "  AND color = 'red'"
      );

    //System.out.println(">>> json: " + json);
    JSONObject timeRange = json.getJSONObject("filter").getJSONObject("range");

    long fromTime = Long.parseLong(timeRange.getJSONObject("time").getString("from"));
    long expectedFromTime = new SimpleDateFormat("yyyy-MM-dd").parse("2012-01-02").getTime();
    assertEquals(fromTime, expectedFromTime);
    assertFalse(timeRange.getJSONObject("time").getBoolean("include_lower"));

    long toTime = Long.parseLong(timeRange.getJSONObject("time").getString("from"));
    long expectedToTime = new SimpleDateFormat("yyyy/MM/dd").parse("2012/01/31").getTime();
    assertEquals(fromTime, expectedFromTime);
    assertTrue(timeRange.getJSONObject("time").getBoolean("include_upper"));
  }

  @Test
  public void testUID() throws Exception
  {
    System.out.println("testUID");
    System.out.println("==================================================");

    long now = System.currentTimeMillis();

    JSONObject json = _compiler.compile(
      "SELECT * " +
      "FROM cars " +
      "WHERE _uid IN (123, 124)"
      );
    JSONObject expected = new JSONObject("{\"selections\":[{\"terms\":{\"_uid\":{\"values\":[123,124],\"excludes\":[],\"operator\":\"or\"}}}],\"meta\":{\"select_list\":[\"*\"]}}");
    assertTrue(_comp.isEquals(json, expected));
  }

  @Test
  public void testLongValue() throws Exception
  {
    System.out.println("testLongValue");
    System.out.println("==================================================");

    JSONObject json = _compiler.compile(
      "SELECT * " +
      "FROM cars " +
      "WHERE long_id IN (5497057336205783040)"
      );
    // System.out.println(">>> json = " + json);
    JSONObject expected = new JSONObject("{\"selections\":[{\"terms\":{\"long_id\":{\"values\":[5497057336205783040],\"excludes\":[],\"operator\":\"or\"}}}],\"meta\":{\"select_list\":[\"*\"]}}");
    assertTrue(_comp.isEquals(json, expected));
  }

  @Test
  public void testCorrectStatement() throws Exception
  {
    System.out.println("testCorrectStatement");
    System.out.println("==================================================");
    //compile the statement

    JSONObject json = _compiler.compile(
      "SELECT color, year " +
      "FROM cars " +
      "WHERE QUERY IS \"hello\" " +
      "  AND color IN (\"red\", \"blue\") EXCEPT ('red') " +
      "  AND category = 'sedan' WITH ('aaa':'111', 'bbb':'222', 'ccc':'333') " +
      "  AND year NOT BETWEEN 1999 AND 2000"
      );
    
    // for (int i = 0; i < ast.getChildCount(); ++i)
    // {
    //   System.out.print(ast.getChild(i).getText() + " -- ");
    // }
    //check AST structure
    //assertEquals(BQLParser.SELECT, ast.getChild(0).getType());
    //assertEquals(BQLParser.STAR, ast.getChild(1).getType());
  }

  @Test
  public void testNullPred1() throws Exception
  {
    System.out.println("testNullPred1");
    System.out.println("==================================================");

    JSONObject json = _compiler.compile(
      "SELECT * " +
      "FROM cars " +
      "WHERE price IS NOT NULL"
      );

    JSONObject expected = new JSONObject("{\"filter\":{\"bool\":{\"must_not\":{\"isNull\":\"price\"}}},\"meta\":{\"select_list\":[\"*\"]}}");
    assertTrue(_comp.isEquals(json, expected));
  }

  @Test
  public void testNullPred2() throws Exception
  {
    System.out.println("testNullPred2");
    System.out.println("==================================================");

    JSONObject json = _compiler.compile(
      "SELECT * " +
      "FROM cars " +
      "WHERE price IS NULL"
      );

    JSONObject expected = new JSONObject("{\"filter\":{\"isNull\":\"price\"},\"meta\":{\"select_list\":[\"*\"]}}");
    assertTrue(_comp.isEquals(json, expected));
  }

  @Test
  public void testRouteBy() throws Exception
  {
    System.out.println("testRouteBy");
    System.out.println("==================================================");

    JSONObject json = _compiler.compile(
      "SELECT * " +
      "FROM cars " +
      "ROUTE BY '1234'"
      );

    JSONObject expected = new JSONObject("{\"routeParam\":\"1234\",\"meta\":{\"select_list\":[\"*\"]}}");
    assertTrue(_comp.isEquals(json, expected));
  }

  @Test
  public void testStringColumnName() throws Exception
  {
    System.out.println("testStringColumnName");
    System.out.println("==================================================");

    JSONObject json = _compiler.compile(
      "SELECT 'color', year " +
      "FROM cars "
      );

    JSONObject expected = new JSONObject("{\"meta\":{\"select_list\":[\"color\",\"year\"]}}");
    assertTrue(_comp.isEquals(json, expected));
  }

  @Test
  public void testSubColumnName() throws Exception
  {
    System.out.println("testSubColumnName");
    System.out.println("==================================================");

    JSONObject json = _compiler.compile(
      "SELECT _srcdata.color, _srcdata.'$time' " +
      "FROM cars "
      );

    JSONObject expected = new JSONObject("{\"fetchStored\":true,\"meta\":{\"select_list\":[\"_srcdata.color\",\"_srcdata.$time\"]}}");
    assertTrue(_comp.isEquals(json, expected));
  }

  @Test
  public void testRelevanceModel1() throws Exception
  {
    System.out.println("testRelevanceModel1");
    System.out.println("==================================================");

    JSONObject json = _compiler.compile(
      "SELECT color, year "                     +
      "FROM cars "                              +
      "WHERE color = 'red' "                    +
      "USING RELEVANCE MODEL homepage_top (srcid:1234)"
      );

    JSONObject expected = new JSONObject("{\"query\":{\"query_string\":{\"query\":\"\",\"relevance\":{\"values\":{\"srcid\":1234},\"predefined_model\":\"homepage_top\"}}},\"selections\":[{\"term\":{\"color\":{\"value\":\"red\"}}}],\"meta\":{\"select_list\":[\"color\",\"year\"]}}");
    assertTrue(_comp.isEquals(json, expected));
  }

  @Test
  public void testRelevanceModelIfStmt() throws Exception
  {
    System.out.println("testRelevanceModelIfStmt");
    System.out.println("==================================================");

    JSONObject json = _compiler.compile(
      "SELECT color, year " +
      "FROM cars " +
      "WHERE color = 'red' " +
      "USING RELEVANCE MODEL my_model (srcid:1234) " +
      "  DEFINED AS (int intParam1, int intParam2, String strParam, int srcid) " +
      "  BEGIN " +
      "    int myInt = 100 + intParam1 + intParam2; " +
      "    String newStr = strParam; " +
      "    if (srcid == myInt + 2) " +
      "      return 123; " +
      "    else if (srcid > 200) " +
      "      return 345; " +
      "    else " +
      "      return _INNER_SCORE; " +
      "  END "
      );

    JSONObject expected = new JSONObject("{\"query\":{\"query_string\":{\"query\":\"\",\"relevance\":{\"model\":{\"function_params\":[\"intParam1\",\"intParam2\",\"strParam\",\"srcid\",\"_INNER_SCORE\"],\"facets\":{},\"variables\":{\"int\":[\"intParam1\",\"intParam2\",\"srcid\"],\"string\":[\"strParam\"]},\"function\":\"int myInt = 100 + intParam1 + intParam2;     String newStr = strParam;     if (srcid == myInt + 2)       return 123;     else if (srcid > 200)       return 345;     else       return _INNER_SCORE;\"},\"values\":{\"srcid\":1234}}}},\"selections\":[{\"term\":{\"color\":{\"value\":\"red\"}}}],\"meta\":{\"select_list\":[\"color\",\"year\"]}}");
    assertTrue(_comp.isEquals(json, expected));
  }

  @Test
  public void testRelevanceModelFloatLiteral() throws Exception
  {
    System.out.println("testRelevanceModelFloatLiteral");
    System.out.println("==================================================");

    JSONObject json = _compiler.compile(
      "SELECT color, year " +
      "FROM cars " +
      "WHERE color = 'red' " +
      "USING RELEVANCE MODEL my_model (srcid:1234) " +
      "  DEFINED AS (int srcid) " +
      "  BEGIN " +
      "    float x1 = 1.2; " +
      "    int x = 7.; " +
      "    x = 7.5e12; " +
      "    x = 7.5e-12; " +
      "    x = 7.5e-12f; " +
      "    x = .34; " +
      "    x = .34e+12; " +
      "    x = 123f; " +
      "    return 0.25; " +
      "  END "
      );

    JSONObject expected = new JSONObject("{\"query\":{\"query_string\":{\"query\":\"\",\"relevance\":{\"model\":{\"function_params\":[\"srcid\"],\"facets\":{},\"variables\":{\"int\":[\"srcid\"]},\"function\":\"float x1 = 1.2;     int x = 7.;     x = 7.5e12;     x = 7.5e-12;     x = 7.5e-12f;     x = .34;     x = .34e+12;     x = 123f;     return 0.25;\"},\"values\":{\"srcid\":1234}}}},\"selections\":[{\"term\":{\"color\":{\"value\":\"red\"}}}],\"meta\":{\"select_list\":[\"color\",\"year\"]}}");
    assertTrue(_comp.isEquals(json, expected));
  }

  @Test
  public void testRelevanceModelDataTypes() throws Exception
  {
    System.out.println("testRelevanceModelDataTypes");
    System.out.println("==================================================");

    JSONObject json = _compiler.compile(
      "SELECT color, year " +
      "FROM cars " +
      "WHERE color = 'red' " +
      "USING RELEVANCE MODEL my_model (srcid:1234) " +
      "  DEFINED AS (int intParam1, int intParam2, String strParam) " +
      "  BEGIN " +
      "    int myInt = 100; " +
      "    String str1; " +
      "    String str2 = \"abcd\"; " +
      "    char ch = 'c'; " +
      "    Integer int1, int2; " +
      "    int int3 = 0L, int4 = 1234l; " +
      "    float f1 = 1.23f, f2 = 1.23F; " +
      "    float e1 = 2e+1234; " +
      "    Byte byte1; " +
      "    IntOpenHashSet mySet1, mySet2; " +
      "    Object2IntOpenHashMap myMap1; " +
      "    return 0.123f; " +
      "  END "
      );

    JSONObject expected = new JSONObject("{\"query\":{\"query_string\":{\"query\":\"\",\"relevance\":{\"model\":{\"function_params\":[\"intParam1\",\"intParam2\",\"strParam\"],\"facets\":{},\"variables\":{\"int\":[\"intParam1\",\"intParam2\"],\"string\":[\"strParam\"]},\"function\":\"int myInt = 100;     String str1;     String str2 = \\\"abcd\\\";     char ch = 'c';     Integer int1, int2;     int int3 = 0L, int4 = 1234l;     float f1 = 1.23f, f2 = 1.23F;     float e1 = 2e+1234;     Byte byte1;     IntOpenHashSet mySet1, mySet2;     Object2IntOpenHashMap myMap1;     return 0.123f;\"},\"values\":{\"srcid\":1234}}}},\"selections\":[{\"term\":{\"color\":{\"value\":\"red\"}}}],\"meta\":{\"select_list\":[\"color\",\"year\"]}}");
    assertTrue(_comp.isEquals(json, expected));
  }

  @Test
  public void testRelevanceModelWhileStmt() throws Exception
  {
    System.out.println("testRelevanceModelWhileStmt");
    System.out.println("==================================================");

    JSONObject json = _compiler.compile(
      "SELECT color, year " +
      "FROM cars " +
      "WHERE color = 'red' " +
      "USING RELEVANCE MODEL my_model (srcid:1234) " +
      "  DEFINED AS (int srcid) " +
      "  BEGIN " +
      "    int myInt = 100; " +
      "    while (myInt < 200) { " +
      "      myInt++; " +
      "      myInt = myInt + 10; " +
      "    } " +
      "    return 100; " +
      "  END "
      );

    JSONObject expected = new JSONObject("{\"query\":{\"query_string\":{\"query\":\"\",\"relevance\":{\"model\":{\"function_params\":[\"srcid\"],\"facets\":{},\"variables\":{\"int\":[\"srcid\"]},\"function\":\"int myInt = 100;     while (myInt < 200) {       myInt++;       myInt = myInt + 10;     }     return 100;\"},\"values\":{\"srcid\":1234}}}},\"selections\":[{\"term\":{\"color\":{\"value\":\"red\"}}}],\"meta\":{\"select_list\":[\"color\",\"year\"]}}");
    assertTrue(_comp.isEquals(json, expected));
  }

  @Test
  public void testRelevanceModelDoWhile() throws Exception
  {
    System.out.println("testRelevanceModelDoWhile");
    System.out.println("==================================================");

    JSONObject json = _compiler.compile(
      "SELECT color, year " +
      "FROM cars " +
      "WHERE color = 'red' " +
      "USING RELEVANCE MODEL my_model (srcid:1234) " +
      "  DEFINED AS (int srcid) " +
      "  BEGIN " +
      "    int myInt = 100; " +
      "    do { " +
      "      myInt = myInt + 10; " +
      "    } while (myInt < 100); " +
      "    return 100; " +
      "  END "
      );

    JSONObject expected = new JSONObject("{\"query\":{\"query_string\":{\"query\":\"\",\"relevance\":{\"model\":{\"function_params\":[\"srcid\"],\"facets\":{},\"variables\":{\"int\":[\"srcid\"]},\"function\":\"int myInt = 100;     do {       myInt = myInt + 10;     } while (myInt < 100);     return 100;\"},\"values\":{\"srcid\":1234}}}},\"selections\":[{\"term\":{\"color\":{\"value\":\"red\"}}}],\"meta\":{\"select_list\":[\"color\",\"year\"]}}");
    assertTrue(_comp.isEquals(json, expected));
  }

  @Test
  public void testRelevanceModelForLoop() throws Exception
  {
    System.out.println("testRelevanceModelForLoop");
    System.out.println("==================================================");

    JSONObject json = _compiler.compile(
      "SELECT color, year " +
      "FROM cars " +
      "WHERE color = 'red' " +
      "USING RELEVANCE MODEL my_model (srcid:1234) " +
      "  DEFINED AS (int srcid) " +
      "  BEGIN " +
      "    int myInt = 0; " +
      "    for (int i = 0; i < 100; i++) { " +
      "      myInt = myInt + i * 10; " +
      "    } " +
      "    return myInt; " +
      "  END "
      );

    JSONObject expected = new JSONObject("{\"query\":{\"query_string\":{\"query\":\"\",\"relevance\":{\"model\":{\"function_params\":[\"srcid\"],\"facets\":{},\"variables\":{\"int\":[\"srcid\"]},\"function\":\"int myInt = 0;     for (int i = 0; i < 100; i++) {       myInt = myInt + i * 10;     }     return myInt;\"},\"values\":{\"srcid\":1234}}}},\"selections\":[{\"term\":{\"color\":{\"value\":\"red\"}}}],\"meta\":{\"select_list\":[\"color\",\"year\"]}}");
    assertTrue(_comp.isEquals(json, expected));
  }

  @Test
  public void testRelevanceModelSwitchStmt() throws Exception
  {
    System.out.println("testRelevanceModelSwitchStmt");
    System.out.println("==================================================");

    JSONObject json = _compiler.compile(
      "SELECT color, year " +
      "FROM cars " +
      "WHERE color = 'red' " +
      "USING RELEVANCE MODEL my_model (srcid:1234) " +
      "  DEFINED AS (int srcid) " +
      "  BEGIN " +
      "    int myInt = 0; " +
      "    switch (myInt) { " +
      "     case 1: myInt = 2; " +
      "             break; " +
      "     case 2:        " +
      "     case 3: myInt = 4; " +
      "             break; " +
      "     default: " +
      "             myInt = 100; " +
      "    } " +
      "    return 0.5f; " +
      "  END "
      );

    JSONObject expected = new JSONObject("{\"query\":{\"query_string\":{\"query\":\"\",\"relevance\":{\"model\":{\"function_params\":[\"srcid\"],\"facets\":{},\"variables\":{\"int\":[\"srcid\"]},\"function\":\"int myInt = 0;     switch (myInt) {      case 1: myInt = 2;              break;      case 2:             case 3: myInt = 4;              break;      default:              myInt = 100;     }     return 0.5f;\"},\"values\":{\"srcid\":1234}}}},\"selections\":[{\"term\":{\"color\":{\"value\":\"red\"}}}],\"meta\":{\"select_list\":[\"color\",\"year\"]}}");
    assertTrue(_comp.isEquals(json, expected));
  }

 

  @Test
  public void testRelevanceModelParameters() throws Exception
  {
    System.out.println("testRelevanceModelParameters");
    System.out.println("==================================================");

    JSONObject json = _compiler.compile(
      "SELECT color, year " +
      "FROM cars " +
      "WHERE color = 'red' " +
      "USING RELEVANCE MODEL my_model (srcid:1234) " +
      "  DEFINED AS (int intParam1, int intParam2, String strParam, " +
      "              DoubleOpenHashSet setParam, Int2IntOpenHashMap mapParam) " +
      "  BEGIN " +
      "    int myInt = 0; " +
      "    float delta = System.currentTimeMillis() + intParam1 + intParam2 ; " +
      "    float t = delta > 0 ? delta : 0; " +
      "    float numHours = t / (1000 * 3600); " +
      "    float timeScore = (float) Math.exp(numHours); " +
      "    if (tags.contains(\"zzz\")) " +
      "      return 999999; " +
      "    int x = 0; " +
      "    x += 5; " +
      "    x *= 10; " +
      "    return timeScore + _INNER_SCORE + price; " +
      "  END "
      );

    JSONObject expected = new JSONObject("{\"query\":{\"query_string\":{\"query\":\"\",\"relevance\":{\"model\":{\"function_params\":[\"intParam1\",\"intParam2\",\"strParam\",\"setParam\",\"mapParam\",\"tags\",\"price\",\"_INNER_SCORE\"],\"facets\":{\"mstring\":[\"tags\"],\"float\":[\"price\"]},\"variables\":{\"map_int_int\":[\"mapParam\"],\"int\":[\"intParam1\",\"intParam2\"],\"string\":[\"strParam\"],\"set_double\":[\"setParam\"]},\"function\":\"int myInt = 0;     float delta = System.currentTimeMillis() + intParam1 + intParam2 ;     float t = delta > 0 ? delta : 0;     float numHours = t / (1000 * 3600);     float timeScore = (float) Math.exp(numHours);     if (tags.contains(\\\"zzz\\\"))       return 999999;     int x = 0;     x += 5;     x *= 10;     return timeScore + _INNER_SCORE + price;\"},\"values\":{\"srcid\":1234}}}},\"selections\":[{\"term\":{\"color\":{\"value\":\"red\"}}}],\"meta\":{\"select_list\":[\"color\",\"year\"]}}");
    assertTrue(_comp.isEquals(json, expected));
  }

  @Test
  public void testRelevanceModelExample1() throws Exception
  {
    System.out.println("testRelevanceModelExample1");
    System.out.println("==================================================");

    JSONObject json = _compiler.compile(
      "SELECT * " +
      "FROM cars " +
      "WHERE color = 'red' " + 
      "USING RELEVANCE MODEL my_model (thisYear:2001, goodYear:[1996]) " +
      "  DEFINED AS (int thisYear, IntOpenHashSet goodYear) " +
      "  BEGIN " +
      "    if (goodYear.contains(year)) " +
      "      return (float)Math.exp(10d); " +
      "    if (year == thisYear) " +
      "      return 87f; " +
      "    return _INNER_SCORE; " +
      "  END " +
      "ORDER BY relevance"
      );

    JSONObject expected = new JSONObject("{\"sort\":\"relevance\",\"query\":{\"query_string\":{\"query\":\"\",\"relevance\":{\"model\":{\"function_params\":[\"thisYear\",\"goodYear\",\"year\",\"_INNER_SCORE\"],\"facets\":{\"int\":[\"year\"]},\"variables\":{\"set_int\":[\"goodYear\"],\"int\":[\"thisYear\"]},\"function\":\"if (goodYear.contains(year))       return (float)Math.exp(10d);     if (year == thisYear)       return 87f;     return _INNER_SCORE;\"},\"values\":{\"thisYear\":2001,\"goodYear\":[1996]}}}},\"selections\":[{\"term\":{\"color\":{\"value\":\"red\"}}}],\"meta\":{\"select_list\":[\"*\"]}}");
    assertTrue(_comp.isEquals(json, expected));
  }

  @Test
  public void testRelevanceModelExample2() throws Exception
  {
    System.out.println("testRelevanceModelExample2");
    System.out.println("==================================================");

    JSONObject json = _compiler.compile(
      "SELECT * " +
      "FROM cars " +
      "WHERE color = 'red' " + 
      "USING RELEVANCE MODEL my_model (thisYear:2001, goodYear:[1996]) " +
      "  DEFINED AS (int thisYear, IntOpenHashSet goodYear) " +
      "  BEGIN " +
      "    if (goodYear.contains(year)) " +
      "      return (float)Math.exp(10d); " +
      "    if (year == thisYear) " +
      "      return 87f; " +
      "    else if (color.equals(\"blue\")) " +
      "      return 99f; " +
      "    return _INNER_SCORE; " +
      "  END " +
      "ORDER BY relevance"
      );

    JSONObject expected = new JSONObject("{\"sort\":\"relevance\",\"query\":{\"query_string\":{\"query\":\"\",\"relevance\":{\"model\":{\"function_params\":[\"thisYear\",\"goodYear\",\"color\",\"year\",\"_INNER_SCORE\"],\"facets\":{\"int\":[\"year\"],\"string\":[\"color\"]},\"variables\":{\"set_int\":[\"goodYear\"],\"int\":[\"thisYear\"]},\"function\":\"if (goodYear.contains(year))       return (float)Math.exp(10d);     if (year == thisYear)       return 87f;     else if (color.equals(\\\"blue\\\"))       return 99f;     return _INNER_SCORE;\"},\"values\":{\"thisYear\":2001,\"goodYear\":[1996]}}}},\"selections\":[{\"term\":{\"color\":{\"value\":\"red\"}}}],\"meta\":{\"select_list\":[\"*\"]}}");
    assertTrue(_comp.isEquals(json, expected));
  }

  @Test
  public void testRelevanceModelVariableScopes() throws Exception
  {
    System.out.println("testRelevanceModelVariableScopes");
    System.out.println("==================================================");

    JSONObject json = _compiler.compile(
      "SELECT * " +
      "FROM cars " +
      "WHERE color = 'red' " + 
      "USING RELEVANCE MODEL my_model (boost:2.5) " +
      "  DEFINED AS (float boost) " +
      "  BEGIN " +
      "    int x, y; " +
      "    for (int i = 0; i < 10; ++i) { " +
      "       x = 10; " +
      "       y = x + i; " +
      "    } " +
      "    return y * boost + price; " +
      "  END " +
      "ORDER BY relevance"
      );

    JSONObject expected = new JSONObject("{\"sort\":\"relevance\",\"query\":{\"query_string\":{\"query\":\"\",\"relevance\":{\"model\":{\"function_params\":[\"boost\",\"price\"],\"facets\":{\"float\":[\"price\"]},\"variables\":{\"float\":[\"boost\"]},\"function\":\"int x, y;     for (int i = 0; i < 10; ++i) {        x = 10;        y = x + i;     }     return y * boost + price;\"},\"values\":{\"boost\":2.5}}}},\"selections\":[{\"term\":{\"color\":{\"value\":\"red\"}}}],\"meta\":{\"select_list\":[\"*\"]}}");
    assertTrue(_comp.isEquals(json, expected));
  }

  @Test
  public void testRelevanceModelMapValue() throws Exception
  {
    System.out.println("testRelevanceModelMapValue");
    System.out.println("==================================================");

    JSONObject json = _compiler.compile(
      "SELECT * " +
      "FROM cars " +
      "WHERE color = 'red' " + 
      "USING RELEVANCE MODEL my_model (thisYear:2001, myMap:{'aaa':1, 'bbb':2}) " +
      "ORDER BY relevance"
      );

    JSONObject expected = new JSONObject("{\"sort\":\"relevance\",\"query\":{\"query_string\":{\"query\":\"\",\"relevance\":{\"predefined_model\":\"my_model\",\"values\":{\"thisYear\":2001,\"myMap\":{\"aaa\":1,\"bbb\":2}}}}},\"selections\":[{\"term\":{\"color\":{\"value\":\"red\"}}}],\"meta\":{\"select_list\":[\"*\"]}}");
    assertTrue(_comp.isEquals(json, expected));
  }
  @Test
  public void testRelevanceModelWithEmptyMapAndList() throws Exception
  {
    System.out.println("testRelevanceModelMapValue");
    System.out.println("==================================================");

    JSONObject json = _compiler.compile(
      "SELECT * " +
      "FROM cars " +
      "WHERE color = 'red' " + 
      "USING RELEVANCE MODEL my_model (thisYear:2001,  list:[], map:{}) " +
      "ORDER BY relevance"
      );

   assertNotNull(json);
  }
  @Test
  public void testRelevanceModelExpressions() throws Exception
  {
    System.out.println("testRelevanceModelExpressions");
    System.out.println("==================================================");

    JSONObject json = _compiler.compile(
      "SELECT color, year " +
      "FROM cars " +
      "WHERE color = 'red' " +
      "USING RELEVANCE MODEL my_model (srcid:1234, timeVal:9999, _half_time:8888, coolTag:'zzz') " +
      "  DEFINED AS (int srcid, long timeVal, long _half_time, String coolTag) " +
      "  BEGIN " +
      "    int myInt = 0; " +
      "    float delta = System.currentTimeMillis() - timeVal; " +
      "    float t = delta > 0 ? delta : 0; " +
      "    float numHours = t / (1000 * 3600); " +
      "    float timeScore = (float) Math.exp(-(numHours/_half_time)); " +
      "    if (tags.contains(coolTag)) " +
      "      return 999999; " +
      "    int x = 0; " +
      "    x += 5; " +
      "    x *= 10; " +
      "    return timeScore; " +
      "  END "
      );
    
    JSONObject expected = new JSONObject("{\"query\":{\"query_string\":{\"query\":\"\",\"relevance\":{\"model\":{\"function_params\":[\"srcid\",\"timeVal\",\"_half_time\",\"coolTag\",\"tags\"],\"facets\":{\"mstring\":[\"tags\"]},\"variables\":{\"int\":[\"srcid\"],\"string\":[\"coolTag\"],\"long\":[\"timeVal\",\"_half_time\"]},\"function\":\"int myInt = 0;     float delta = System.currentTimeMillis() - timeVal;     float t = delta > 0 ? delta : 0;     float numHours = t / (1000 * 3600);     float timeScore = (float) Math.exp(-(numHours/_half_time));     if (tags.contains(coolTag))       return 999999;     int x = 0;     x += 5;     x *= 10;     return timeScore;\"},\"values\":{\"_half_time\":8888,\"timeVal\":9999,\"coolTag\":\"zzz\",\"srcid\":1234}}}},\"selections\":[{\"term\":{\"color\":{\"value\":\"red\"}}}],\"meta\":{\"select_list\":[\"color\",\"year\"]}}");
    assertTrue(_comp.isEquals(json, expected));
  }
  @Test
  public void testRelevanceModelExpressionsWithtemplateMappings() throws Exception
  {
    System.out.println("testRelevanceModelExpressions");
    System.out.println("==================================================");
    
    String bqlStmt = "SELECT color, year " +
    "FROM cars " +
    "WHERE color = $color " +
    "USING RELEVANCE MODEL my_model ($params) " +
    "  DEFINED AS (int srcid, long timeVal, long _half_time, String coolTag) " +
    "  BEGIN " +
    "    int myInt = 0; " +
    "    float delta = System.currentTimeMillis() - timeVal; " +
    "    float t = delta > 0 ? delta : 0; " +
    "    float numHours = t / (1000 * 3600); " +
    "    float timeScore = (float) Math.exp(-(numHours/_half_time)); " +
    "    if (tags.contains(coolTag)) " +
    "      return 999999; " +
    "    int x = 0; " +
    "    x += 5; " +
    "    x *= $ten; " +
    "    return timeScore; " +
    "  END ";
    String templateMappings = "{\"templateMapping\": {    \"color\": \"'red'\",    \"ten\": 10, \"params\":\"srcid:1234, timeVal:9999, _half_time:8888, coolTag:'zzz'\"  }}";
    JsonTemplateProcessor jsonTemplateProcessor = new JsonTemplateProcessor();
    String modifiedJson = (String) jsonTemplateProcessor.process(bqlStmt, jsonTemplateProcessor.getTemplates(new JSONObject(templateMappings)));
    JSONObject json = _compiler.compile(modifiedJson);
    JSONObject expected = new JSONObject("{\"query\":{\"query_string\":{\"query\":\"\",\"relevance\":{\"model\":{\"function_params\":[\"srcid\",\"timeVal\",\"_half_time\",\"coolTag\",\"tags\"],\"facets\":{\"mstring\":[\"tags\"]},\"variables\":{\"int\":[\"srcid\"],\"string\":[\"coolTag\"],\"long\":[\"timeVal\",\"_half_time\"]},\"function\":\"int myInt = 0;     float delta = System.currentTimeMillis() - timeVal;     float t = delta > 0 ? delta : 0;     float numHours = t / (1000 * 3600);     float timeScore = (float) Math.exp(-(numHours/_half_time));     if (tags.contains(coolTag))       return 999999;     int x = 0;     x += 5;     x *= 10;     return timeScore;\"},\"values\":{\"_half_time\":8888,\"timeVal\":9999,\"coolTag\":\"zzz\",\"srcid\":1234}}}},\"selections\":[{\"term\":{\"color\":{\"value\":\"red\"}}}],\"meta\":{\"select_list\":[\"color\",\"year\"]}}");
    assertTrue(_comp.isEquals(json, expected));
  }
}
