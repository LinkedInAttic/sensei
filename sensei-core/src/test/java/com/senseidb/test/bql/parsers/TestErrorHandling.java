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

import java.text.ParseException;
import java.text.SimpleDateFormat;

public class TestErrorHandling extends TestCase
{

  private BQLCompiler _compiler;
  private JsonComparator _comp = new JsonComparator(1);

  public TestErrorHandling()
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
    facetInfoMap.put("time", new String[]{"custom", ""}); // Mimic a custom facet
    _compiler = new BQLCompiler(facetInfoMap);
  }

  @Test
  public void testBasicError1() throws Exception
  {
    System.out.println("testBasicError1");
    System.out.println("==================================================");

    boolean caughtException = false;
    try 
    {
      // Incomplete where clause
      JSONObject json = _compiler.compile(
        "select category " +
        "from cars " +
        "where"
        );
    }
    catch (RecognitionException err)
    {
      assertEquals("[line:1, col:31] No viable alternative (token=<EOF>)",
                   _compiler.getErrorMessage(err));
      caughtException = true;
    }
    finally 
    {
      assertTrue(caughtException);
    }
  }

  @Test
  public void testInconsistentRanges() throws Exception
  {
    System.out.println("testInconsistentRanges");
    System.out.println("==================================================");

    boolean caughtException = false;
    try 
    {
      JSONObject json = _compiler.compile(
        "SELECT category \n" +
        "FROM cars \n" +
        "WHERE color = 'red' \n" +
        "  AND year > 2000 AND year < 1995 \n" +
        "  OR price < 1750.00"
        );
    }
    catch (RecognitionException err)
    {
      assertEquals("[line:5, col:2] Inconsistent ranges detected for column: year (token=OR)",
                   _compiler.getErrorMessage(err));
      caughtException = true;
    }
    finally 
    {
      assertTrue(caughtException);
    }
  }

  @Test
  public void testInvalidInPred() throws Exception
  {
    System.out.println("testInvalidInPred");
    System.out.println("==================================================");

    boolean caughtException = false;
    try 
    {
      JSONObject json = _compiler.compile(
        "SELECT category \n" +
        "FROM cars \n" +
        "WHERE year in (1995, 2000) \n" +
        "  AND price < 1750.00"
        );
    }
    catch (RecognitionException err)
    {
      assertEquals("[line:4, col:2] Range facet \"year\" cannot be used in IN predicates. (token=AND)",
                   _compiler.getErrorMessage(err));
      caughtException = true;
    }
    finally 
    {
      assertTrue(caughtException);
    }
  }

  @Test
  public void testInvalidInPredValues() throws Exception
  {
    System.out.println("testInvalidInPredValues");
    System.out.println("==================================================");

    boolean caughtException = false;
    try 
    {
      JSONObject json = _compiler.compile(
        "SELECT category \n" +
        "FROM cars \n" +
        "WHERE color in ('red', 2000, 'blue') \n" +
        "  AND price < 1750.00"
        );
    }
    catch (RecognitionException err)
    {
      assertEquals("[line:4, col:2] Value list for IN predicate of facet \"color\" contains incompatible value(s). (token=AND)",
                   _compiler.getErrorMessage(err));
      caughtException = true;
    }
    finally 
    {
      assertTrue(caughtException);
    }
  }

  @Test
  public void testInvalidInPredExceptValues() throws Exception
  {
    System.out.println("testInvalidInPredExceptValues");
    System.out.println("==================================================");

    boolean caughtException = false;
    try 
    {
      JSONObject json = _compiler.compile(
        "SELECT category \n" +
        "FROM cars \n" +
        "WHERE color IN ('red', 'blue') EXCEPT ('black', 2000) \n" +
        "  AND price < 1750.00"
        );
    }
    catch (RecognitionException err)
    {
      assertEquals("[line:4, col:2] EXCEPT value list for IN predicate of facet \"color\" contains incompatible value(s). (token=AND)",
                   _compiler.getErrorMessage(err));
      caughtException = true;
    }
    finally 
    {
      assertTrue(caughtException);
    }
  }

  @Test
  public void testInvalidContainsAllPred() throws Exception
  {
    System.out.println("testInvalidContainsAllPred");
    System.out.println("==================================================");

    boolean caughtException = false;
    try 
    {
      JSONObject json = _compiler.compile(
        "SELECT category \n" +
        "FROM cars \n" +
        "WHERE year contains all (1995, 2000) \n" +
        "  AND price < 1750.00"
        );
    }
    catch (RecognitionException err)
    {
      assertEquals("[line:4, col:2] Range facet column \"year\" cannot be used in CONTAINS ALL predicates. (token=AND)",
                   _compiler.getErrorMessage(err));
      caughtException = true;
    }
    finally 
    {
      assertTrue(caughtException);
    }
  }

  @Test
  public void testInvalidContainsAllPredValues() throws Exception
  {
    System.out.println("testInvalidContainsAllPredValues");
    System.out.println("==================================================");

    boolean caughtException = false;
    try 
    {
      JSONObject json = _compiler.compile(
        "SELECT category \n" +
        "FROM cars \n" +
        "WHERE tags CONTAINS ALL ('cool', 175.50, 'hybrid') \n" +
        "  AND price < 1750.00"
        );
    }
    catch (RecognitionException err)
    {
      assertEquals("[line:4, col:2] Value list for CONTAINS ALL predicate of facet \"tags\" contains incompatible value(s). (token=AND)",
                   _compiler.getErrorMessage(err));
      caughtException = true;
    }
    finally 
    {
      assertTrue(caughtException);
    }
  }

  @Test
  public void testInvalidContainsAllPredExceptValues() throws Exception
  {
    System.out.println("testInvalidContainsAllPredExceptValues");
    System.out.println("==================================================");

    boolean caughtException = false;
    try 
    {
      JSONObject json = _compiler.compile(
        "SELECT category \n" +
        "FROM cars \n" +
        "WHERE tags contains all ('cool', 'hybrid') EXCEPT ('moon-roof', 2000) \n" +
        "  AND price < 1750.00"
        );
    }
    catch (RecognitionException err)
    {
      assertEquals("[line:4, col:2] EXCEPT value list for CONTAINS ALL predicate of facet \"tags\" contains incompatible value(s). (token=AND)",
                   _compiler.getErrorMessage(err));
      caughtException = true;
    }
    finally 
    {
      assertTrue(caughtException);
    }
  }

  @Test
  public void testBadDataInEqualPred() throws Exception
  {
    System.out.println("testBadDataInEqualPred");
    System.out.println("==================================================");

    boolean caughtException = false;
    try 
    {
      JSONObject json = _compiler.compile(
        "SELECT category \n" +
        "FROM cars \n" +
        "WHERE color = 1234 \n" +
        "  AND price < 1750.00"
        );
    }
    catch (RecognitionException err)
    {
      assertEquals("[line:4, col:2] Incompatible data type was found in an EQUAL predicate for column \"color\". (token=AND)",
                   _compiler.getErrorMessage(err));
      caughtException = true;
    }
    finally 
    {
      assertTrue(caughtException);
    }
  }

  @Test
  public void testExpectingCOLON() throws Exception
  {
    System.out.println("testExpectingCOLON");
    System.out.println("==================================================");

    boolean caughtException = false;
    try 
    {
      JSONObject json = _compiler.compile(
        "SELECT category \n" +
        "FROM cars \n" +
        "WHERE city = 'u.s.a./new york' WITH('strict', true) \n" +
        "  AND price < 1750.00"
        );
    }
    catch (RecognitionException err)
    {
      assertEquals("[line:3, col:44] Expecting COLON (token=,)",
                   _compiler.getErrorMessage(err));
      caughtException = true;
    }
    finally 
    {
      assertTrue(caughtException);
    }
  }

  @Test
  public void testUnsupportedProp() throws Exception
  {
    System.out.println("testUnsupportedProp");
    System.out.println("==================================================");

    boolean caughtException = false;
    try 
    {
      JSONObject json = _compiler.compile(
        "SELECT category \n" +
        "FROM cars \n" +
        "WHERE city = 'u.s.a./new york' WITH('ddd':123, 'strict':true) \n" +
        "  AND price < 1750.00"
        );
    }
    catch (RecognitionException err)
    {
      assertEquals("[line:4, col:2] Unsupported property was found in an EQUAL predicate for path facet column \"city\": ddd. (token=AND)",
                   _compiler.getErrorMessage(err));
      caughtException = true;
    }
    finally 
    {
      assertTrue(caughtException);
    }
  }

  @Test
  public void testBadDataInNotEqualPred() throws Exception
  {
    System.out.println("testBadDataInNotEqualPred");
    System.out.println("==================================================");

    boolean caughtException = false;
    try 
    {
      JSONObject json = _compiler.compile(
        "SELECT category \n" +
        "FROM cars \n" +
        "WHERE color <> 1234 \n" +
        "  AND price < 1750.00"
        );
    }
    catch (RecognitionException err)
    {
      assertEquals("[line:4, col:2] Incompatible data type was found in a NOT EQUAL predicate for column \"color\". (token=AND)",
                   _compiler.getErrorMessage(err));
      caughtException = true;
    }
    finally 
    {
      assertTrue(caughtException);
    }
  }

  @Test
  public void testNotEqualOnPath() throws Exception
  {
    System.out.println("testNotEqualOnPath");
    System.out.println("==================================================");

    boolean caughtException = false;
    try 
    {
      JSONObject json = _compiler.compile(
        "SELECT category \n" +
        "FROM cars \n" +
        "WHERE city <> 'u.s.a./new york' \n" +
        "  AND price < 1750.00"
        );
    }
    catch (RecognitionException err)
    {
      assertEquals("[line:4, col:2] NOT EQUAL predicate is not supported for path facets (column \"city\"). (token=AND)",
                   _compiler.getErrorMessage(err));
      caughtException = true;
    }
    finally 
    {
      assertTrue(caughtException);
    }
  }

  @Test
  public void testBadBetweenPred() throws Exception
  {
    System.out.println("testBadBetweenPred");
    System.out.println("==================================================");

    boolean caughtException = false;
    try 
    {
      JSONObject json = _compiler.compile(
        "SELECT category \n" +
        "FROM cars \n" +
        "WHERE color BETWEEN 'blue' AND 'red' \n" +
        "  AND price < 1750.00"
        );
    }
    catch (RecognitionException err)
    {
      assertEquals("[line:4, col:2] Non-range facet column \"color\" cannot be used in BETWEEN predicates. (token=AND)",
                   _compiler.getErrorMessage(err));
      caughtException = true;
    }
    finally 
    {
      assertTrue(caughtException);
    }
  }

  @Test
  public void testBadDataInBetweenPred() throws Exception
  {
    System.out.println("testBadDataInBetweenPred");
    System.out.println("==================================================");

    boolean caughtException = false;
    try 
    {
      JSONObject json = _compiler.compile(
        "SELECT category \n" +
        "FROM cars \n" +
        "WHERE year BETWEEN 'blue' AND 2000 \n" +
        "  AND price < 1750.00"
        );
    }
    catch (RecognitionException err)
    {
      assertEquals("[line:4, col:2] Incompatible data type was found in a BETWEEN predicate for column \"year\". (token=AND)",
                   _compiler.getErrorMessage(err));
      caughtException = true;
    }
    finally 
    {
      assertTrue(caughtException);
    }
  }

  @Test
  public void testBadRangePred() throws Exception
  {
    System.out.println("testBadRangePred");
    System.out.println("==================================================");

    boolean caughtException = false;
    try 
    {
      JSONObject json = _compiler.compile(
        "SELECT category \n" +
        "FROM cars \n" +
        "WHERE color > 'red' \n" +
        "  AND price < 1750.00"
        );
    }
    catch (RecognitionException err)
    {
      assertEquals("[line:4, col:2] Non-range facet column \"color\" cannot be used in RANGE predicates. (token=AND)",
                   _compiler.getErrorMessage(err));
      caughtException = true;
    }
    finally 
    {
      assertTrue(caughtException);
    }
  }

  @Test
  public void testBadDataInRangePred() throws Exception
  {
    System.out.println("testBadDataInRangePred");
    System.out.println("==================================================");

    boolean caughtException = false;
    try 
    {
      JSONObject json = _compiler.compile(
        "SELECT category \n" +
        "FROM cars \n" +
        "WHERE year > 'red' \n" +
        "  AND price < 1750.00"
        );
    }
    catch (RecognitionException err)
    {
      assertEquals("[line:4, col:2] Incompatible data type was found in a RANGE predicate for column \"year\". (token=AND)",
                   _compiler.getErrorMessage(err));
      caughtException = true;
    }
    finally 
    {
      assertTrue(caughtException);
    }
  }

  @Test
  public void testBadDatetime1() throws Exception
  {
    System.out.println("testBadDatetime1");
    System.out.println("==================================================");

    boolean caughtException = false;
    try 
    {
      JSONObject json = _compiler.compile(
        "SELECT category \n" +
        "FROM cars \n" +
        "WHERE time > 2011-16-20 55:10:10 \n" +
        "  AND price < 1750.00"
        );
    }
    catch (RecognitionException err)
    {
      assertEquals("[line:4, col:2] Date string contains invalid date/time: \"2011-16-20 55:10:10\". (token=AND)",
                   _compiler.getErrorMessage(err));
      caughtException = true;
    }
    finally 
    {
      assertTrue(caughtException);
    }
  }

  @Test
  public void testBadDatetime2() throws Exception
  {
    System.out.println("testBadDatetime2");
    System.out.println("==================================================");

    boolean caughtException = false;
    try 
    {
      JSONObject json = _compiler.compile(
        "SELECT category \n" +
        "FROM cars \n" +
        "WHERE time > 2011-10/20 \n" +
        "  AND price < 1750.00"
        );
    }
    catch (RecognitionException err)
    {
      assertEquals("[line:4, col:2] ParseException happened for \"2011-10/20\": Unparseable date: \"2011-10/20\". (token=AND)",
                   _compiler.getErrorMessage(err));
      caughtException = true;
    }
    finally 
    {
      assertTrue(caughtException);
    }
  }

  @Test
  public void testBadMatchPred() throws Exception
  {
    System.out.println("testBadMatchPred");
    System.out.println("==================================================");

    boolean caughtException = false;
    try
    {
      JSONObject json = _compiler.compile(
        "SELECT color \n" +
        "FROM cars \n" +
        "WHERE MATCH(color, year) AGAINST('text1 AND text2') \n" +
        "  AND price < 1750.00"
        );
    }
    catch (RecognitionException err)
    {
      assertEquals("[line:4, col:2] Non-string type column \"year\" cannot be used in MATCH AGAINST predicates. (token=AND)",
                   _compiler.getErrorMessage(err));
      caughtException = true;
    }
    finally 
    {
      assertTrue(caughtException);
    }
  }

  @Test
  public void testEOF() throws Exception
  {
    System.out.println("testEOF");
    System.out.println("==================================================");

    boolean caughtException = false;
    try
    {
      JSONObject json = _compiler.compile(
        "select color, year from where year > 1"
        );
    }
    catch (RecognitionException err)
    {
      assertEquals("[line:1, col:24] Expecting IDENT (token=where)",
                   _compiler.getErrorMessage(err));
      caughtException = true;
    }
    finally 
    {
      assertTrue(caughtException);
    }
  }

  @Test
  public void testBadSelectList() throws Exception
  {
    System.out.println("testBadSelectList");
    System.out.println("==================================================");

    boolean caughtException = false;
    try
    {
      JSONObject json = _compiler.compile(
        "select color, from aa where color  = 'red'"
        );
    }
    catch (RecognitionException err)
    {
      assertEquals("[line:1, col:14] Expecting IDENT (token=from)",
                   _compiler.getErrorMessage(err));
      caughtException = true;
    }
    finally 
    {
      assertTrue(caughtException);
    }
  }

  @Test
  public void testOrderByOnce() throws Exception
  {
    System.out.println("testOrderByOnce");
    System.out.println("==================================================");

    boolean caughtException = false;
    try
    {
      JSONObject json = _compiler.compile(
        "select category \n" +
        "from cars \n" +
        "order by color \n" +
        "order by year \n" +
        "limit 10"
        );
    }
    catch (RecognitionException err)
    {
      assertEquals("[line:5, col:0] ORDER BY clause can only appear once. (token=limit)",
                   _compiler.getErrorMessage(err));
      caughtException = true;
    }
    finally 
    {
      assertTrue(caughtException);
    }
  }

  @Test
  public void testLimitOnce() throws Exception
  {
    System.out.println("testLimitOnce");
    System.out.println("==================================================");

    boolean caughtException = false;
    try
    {
      JSONObject json = _compiler.compile(
        "select category \n" +
        "from cars \n" +
        "limit 10, 20 \n" +
        "limit 10 \n" +
        "order by color \n"
        );
    }
    catch (RecognitionException err)
    {
      assertEquals("[line:5, col:0] LIMIT clause can only appear once. (token=order)",
                   _compiler.getErrorMessage(err));
      caughtException = true;
    }
    finally 
    {
      assertTrue(caughtException);
    }
  }

  @Test
  public void testBadGroupBy() throws Exception
  {
    System.out.println("testBadGroupBy");
    System.out.println("==================================================");

    boolean caughtException = false;
    try
    {
      JSONObject json = _compiler.compile(
        "select category, tags \n" +
        "from cars \n" +
        "group by tags \n" +
        "order by color \n"
        );
    }
    catch (RecognitionException err)
    {
      assertEquals("[line:4, col:0] Range/multi/path facet, \"tags\", cannot be used in the GROUP BY clause. (token=order)",
                   _compiler.getErrorMessage(err));
      caughtException = true;
    }
    finally 
    {
      assertTrue(caughtException);
    }
  }

  // @Test
  // public void testConflictSelections() throws Exception
  // {
  //   System.out.println("testConflictSelections");
  //   System.out.println("==================================================");

  //   boolean caughtException = false;
  //   try
  //   {
  //     JSONObject json = _compiler.compile(
  //       "SELECT color FROM cars WHERE color = 'red' AND color = 'blue'"
  //       );
  //   }
  //   catch (RecognitionException err)
  //   {
  //     assertEquals("[line:1, col:14] Expecting IDENT (token=from)",
  //                  _compiler.getErrorMessage(err));
  //     caughtException = true;
  //   }
  //   finally 
  //   {
  //     assertTrue(caughtException);
  //   }
  // }
  
  @Test
  public void testBadTimePredicate() throws Exception
  {
    System.out.println("testBadTimePredicate");
    System.out.println("==================================================");

    boolean caughtException = false;
    try
    {
      JSONObject json = _compiler.compile(
        "select category \n" +
        "from cars \n" +
        "where color IN LAST 2 days"
        );
    }
    catch (RecognitionException err)
    {
      assertEquals("[line:3, col:26] Non-range facet column \"color\" cannot be used in TIME predicates. (token=<EOF>)",
                   _compiler.getErrorMessage(err));
      caughtException = true;
    }
    finally 
    {
      assertTrue(caughtException);
    }
  }

  @Test
  public void testOverflowInteger() throws Exception
  {
    System.out.println("testOverflowInteger");
    System.out.println("==================================================");

    boolean caughtException = false;
    try
    {
      JSONObject json = _compiler.compile(
        "select category \n" +
        "from cars \n" +
        "where year = 12345678901234567890"
        );
    }
    catch (RecognitionException err)
    {
      assertEquals("[line:3, col:33] Hit NumberFormatException: For input string: \"12345678901234567890\" (token=<EOF>)",
                   _compiler.getErrorMessage(err));
      caughtException = true;
    }
    finally 
    {
      assertTrue(caughtException);
    }
  }

}
