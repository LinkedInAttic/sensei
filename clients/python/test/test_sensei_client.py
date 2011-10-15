import sys
import unittest

sys.path.insert(0, "../src")
import sensei_client
from sensei_client import *
from pyparsing import ParseException

class TestSenseiClient(unittest.TestCase):
  """Test cases for Sensei client."""

  def setUp(self):
    self.client = SenseiClient()

  def testBasics(self):
    req = SenseiRequest()
    req.offset = 15
    req.count = 20
    self.compare("start=15&rows=20",
                 SenseiClient.buildUrlString(req))

  def testSort(self):
    req = SenseiRequest()
    req.offset = 15
    req.count = 20
    sort1 = SenseiSort("price")
    req.sorts = [sort1]
    self.compare("sort=price%3Aasc&start=15&rows=20",
                 SenseiClient.buildUrlString(req))
    sort2 = SenseiSort("price", True)
    req.sorts = [sort2]
    self.compare("sort=price%3Adesc&start=15&rows=20",
                 SenseiClient.buildUrlString(req))

  def testSortTwoColumns(self):
    req = SenseiRequest()
    req.offset = 15
    req.count = 20
    sort1 = SenseiSort("year", True)
    sort2 = SenseiSort("relevance")
    req.sorts = [sort1, sort2]
    self.compare("sort=year%3Adesc%2Crelevance&start=15&rows=20",
                 SenseiClient.buildUrlString(req))

  def testSortByRelevance(self):
    req = SenseiRequest()
    req.offset = 15
    req.count = 20
    sort1 = SenseiSort("relevance")
    req.sorts = [sort1]
    self.compare("sort=relevance&start=15&rows=20",
                 SenseiClient.buildUrlString(req))

  def testQueryParam(self):
    req = SenseiRequest()
    req.offset = 0
    req.count = 4
    sort1 = SenseiSort("relevance")
    req.sorts = [sort1]
    qParam = {}
    qParam["query"] = "cool car"
    qParam["param1"] = "value1"
    qParam["param2"] = "value2"
    req.qParam = qParam
    self.compare("sort=relevance&start=0&rows=4&q=cool+car&qparam=param2%3Avalue2%2Cparam1%3Avalue1",
                 SenseiClient.buildUrlString(req))

  def testSelection(self):
    req = SenseiRequest()
    req.offset = 0
    req.count = 3
    
    select1 = SenseiSelection("color", "or")
    select1.addSelection("red")
    select1.addSelection("yellow")
    select1.addSelection("black", True)
    select1.addProperty("aaa", "111")
    select1.addProperty("bbb", "222")
    
    select2 = SenseiSelection("price")
    select2.addSelection("[* TO 6700]")
    select2.addSelection("[10000 TO 13100]")
    select2.addSelection("[13200 TO 17300]")
    
    req.selections["color"] = select1
    req.selections["price"] = select2
    self.compare("select.color.val=red%2Cyellow&rows=3&select.color.op=or" +
                 "&select.price.not=&start=0&select.color.not=black" +
                 "&select.color.prop=aaa%3A111%2Cbbb%3A222&select.price.op=or" +
                 "&select.price.val=%5B%2A+TO+6700%5D%2C%5B10000+TO+13100%5D%2C%5B13200+TO+17300%5D",
                 SenseiClient.buildUrlString(req))

  def testFacetSpecs(self):
    req = SenseiRequest()
    req.offset = 0
    req.count = 10
  
    facet1 = SenseiFacet()
    facet2 = SenseiFacet(True, 1, 3, PARAM_FACET_ORDER_VAL)
    facet3 = SenseiFacet(True, 1, 3, PARAM_FACET_ORDER_VAL)
  
    req.facets["year"] = facet1
    req.facets["color"] = facet2
    req.facets["price"] = facet3
    req.facets["city"] = facet3
    req.facets["category"] = facet3

    self.compare("facet.category.minhit=1&facet.color.order=val" +
                 "&facet.price.order=val&facet.price.minhit=1" +
                 "&facet.price.expand=true&facet.city.max=3" +
                 "&facet.color.expand=true&rows=10&start=0" +
                 "&facet.category.max=3&facet.price.max=3" +
                 "&facet.year.minhit=1&facet.year.max=10" +
                 "&facet.year.expand=false&facet.category.order=val" +
                 "&facet.city.minhit=1&facet.color.max=3" +
                 "&facet.city.order=val&facet.color.minhit=1" +
                 "&facet.year.order=hits&facet.city.expand=true" +
                 "&facet.category.expand=true",
                 SenseiClient.buildUrlString(req))

  def testInitParams(self):
    req = SenseiRequest()
  
    param1 = SenseiFacetInitParams()
    param1.put_int_param("srcid", [12345])
    param1.put_bool_param("is_member", False)
  
    param2 = SenseiFacetInitParams()
    param2.put_int_param("time", 999999)
    param2.put_string_param("week", "Monday")
  
    req.facet_init_param_map["My-Network"] = param1
    req.facet_init_param_map["Trending"] = param2
  
    self.compare("rows=10&dyn.My-Network.srcid.type=int" +
                 "&dyn.Trending.week.type=string&dyn.Trending.week.vals=Monday" +
                 "&dyn.Trending.time.type=int&dyn.My-Network.is_member.vals=false" +
                 "&start=0&dyn.My-Network.is_member.type=boolean" +
                 "&dyn.My-Network.srcid.vals=12345&dyn.Trending.time.vals=999999",
                 SenseiClient.buildUrlString(req))

  def compare(self, paramStr1, paramStr2):
    """Compare two URL param strings built by Sensei client.

    Make sure two strings are equivalent.

    """

    list1 = paramStr1.split('&')
    list1.sort()
    list2 = paramStr2.split('&')
    list2.sort()
    self.assertTrue(len(paramStr1) == len(paramStr2) and list1 == list2)


class TestBQL(unittest.TestCase):
  """ Test cases for BQL."""

  def setUp(self):
    self.client = SenseiClient()

  def testBasics(self):
    stmt = \
    """
    SELECT color, price
    FROM cars
    WHERE color in ("red", "blue");
    """
    req = SenseiRequest(stmt)
    self.assertEqual(req.columns, ["color", "price"])
    self.assertEqual(req.offset, sensei_client.DEFAULT_REQUEST_OFFSET)
    self.assertEqual(req.count, sensei_client.DEFAULT_REQUEST_COUNT)
    self.assertEqual(req.query, "")
    self.assertEqual(req.sorts, [])
    self.assertEqual(len(req.selections), 1)
    select = req.selections["color"]
    self.assertEqual(select.field, "color")
    self.assertEqual(select.operation, sensei_client.PARAM_SELECT_OP_OR)
    self.assertEqual(select.values, ["red", "blue"])

  def testNotIn(self):
    stmt = \
    """
    SELECT color, price
    FROM cars
    WHERE color NOT IN ("yellow", "green");
    """
    req = SenseiRequest(stmt)
    self.assertEqual(len(req.selections), 1)
    select = req.selections["color"]
    self.assertEqual(select.field, "color")
    self.assertEqual(select.operation, sensei_client.PARAM_SELECT_OP_OR)
    self.assertEqual(select.excludes, ["yellow", "green"])

  def testLiterals(self):
    stmt = \
    """
    SELECT *
    FROM people
    WHERE age in (20, 30, "40")         -- Now we accept both string and integer
      AND last_name = "Cui"
    """
    req = SenseiRequest(stmt)
    self.assertEqual(len(req.selections), 2)
    select = req.selections["age"]
    self.assertEqual(select.field, "age")
    self.assertEqual(select.operation, sensei_client.PARAM_SELECT_OP_OR)
    self.assertEqual(select.values, ["20", "30", "40"])
    select = req.selections["last_name"]
    self.assertEqual(select.field, "last_name")
    self.assertEqual(select.operation, sensei_client.PARAM_SELECT_OP_AND)
    self.assertEqual(select.values, ["Cui"])

  def testOrderBy(self):
    stmt = \
    """
    SELECT *
    FROM cars
    ORDER BY year desc, price
    """
    req = SenseiRequest(stmt)
    self.assertEqual(len(req.sorts), 2)
    self.assertEqual(str(req.sorts[0]), "year:desc")
    self.assertEqual(str(req.sorts[1]), "price:asc")

    stmt = \
    """
    SELECT *
    FROM cars
    ORDER BY relevance
    """
    req = SenseiRequest(stmt)
    self.assertEqual(len(req.sorts), 1)
    self.assertEqual(str(req.sorts[0]), "relevance")

    stmt = \
    """
    SELECT *
    FROM cars
    ORDER BY relevance desc
    """
    # Make sure we do not allow anything to follow "relevance"
    intactFlag = True
    try:
      req = SenseiRequest(stmt)
      intactFlag = False
    except ParseException as err:
      pass
    finally:
      self.assertTrue(intactFlag)


  def testWhereConditions(self):
    stmt = \
    """
    SELECT color, year, tags, price
    FROM cars
    WHERE query is "cool"
      AND color in ("gold", "green", "blue") EXCEPT ("black")
      AND year in ("[1996 TO 1997]", "[2002 TO 2003]") WITH ("aaa":"111", "bbb":"222")
      and tags contains all ("hybrid", "favorite")
    ORDER BY price desc
    LIMIT 5, 20
    """
    req = SenseiRequest(stmt)
    self.assertEqual(req.columns, ["color", "year", "tags", "price"])
    self.assertEqual(req.offset, 5)
    self.assertEqual(req.count, 20)
    self.assertEqual(req.query, "cool")
    self.assertEqual(len(req.sorts), 1)
    sort = req.sorts[0]
    self.assertEqual(sort.field, "price")
    self.assertEqual(sort.dir, sensei_client.PARAM_SORT_DESC)

    select_color = req.selections["color"]
    self.assertEqual(select_color.field, "color")
    self.assertEqual(select_color.operation, sensei_client.PARAM_SELECT_OP_OR)
    self.assertEqual(select_color.values, ["gold", "green", "blue"])
    self.assertEqual(select_color.excludes, ["black"])
    select_year = req.selections["year"]
    self.assertEqual(select_year.field, "year")
    self.assertEqual(select_year.operation, sensei_client.PARAM_SELECT_OP_OR)
    self.assertEqual(select_year.values, ["[1996 TO 1997]", "[2002 TO 2003]"])
    self.assertEqual(select_year.properties["aaa"], "111")
    self.assertEqual(select_year.properties["bbb"], "222")
    select_tags = req.selections["tags"]
    self.assertEqual(select_tags.field, "tags")
    self.assertEqual(select_tags.operation, sensei_client.PARAM_SELECT_OP_AND)
    self.assertEqual(select_tags.values, ["hybrid", "favorite"])
    self.assertEqual(select_tags.excludes, [])

  def testGroupBy(self):
    stmt = \
    """
    SELECT *
    FROM cars
    GROUP BY color
    """
    req = SenseiRequest(stmt)
    self.assertEqual(req.groupby, "color")
    self.assertEqual(req.max_per_group, sensei_client.DEFAULT_REQUEST_MAX_PER_GROUP)

    stmt = \
    """
    SELECT *
    FROM cars
    GROUP     BY color top 3
    """
    req = SenseiRequest(stmt)
    self.assertEqual(req.groupby, "color")
    self.assertEqual(req.max_per_group, 3)

    stmt = \
    """
    SELECT *
    FROM cars
    GROUP BY color top 3
    """
    req = SenseiRequest(stmt)
    self.assertEqual(req.groupby, "color")
    self.assertEqual(req.max_per_group, 3)

  def testBrowseBy(self):
    stmt = \
    """
    SELECT *
    FROM cars
    BROWSE BY color(false, 1, 10, hits), price(true, 1, 20, value)
    """
    req = SenseiRequest(stmt)
    self.assertEqual(req.columns, ["*"])
    self.assertEqual(req.offset, 0)
    self.assertEqual(req.count, 10)
    self.assertEqual(req.query, "")
    self.assertEqual(len(req.facets), 2)
    facet_color = req.facets["color"]
    self.assertEqual(facet_color.expand, False)
    self.assertEqual(facet_color.minHits, 1)
    self.assertEqual(facet_color.maxCounts, 10)
    self.assertEqual(facet_color.orderBy, sensei_client.PARAM_FACET_ORDER_HITS)
    facet_price = req.facets["price"]
    self.assertEqual(facet_price.expand, True)
    self.assertEqual(facet_price.minHits, 1)
    self.assertEqual(facet_price.maxCounts, 20)
    self.assertEqual(facet_price.orderBy, sensei_client.PARAM_FACET_ORDER_VAL)

  def testBrowseBy2(self):
    stmt = \
    """
    SELECT *
    FROM cars
    BROWSE BY color, price(true, 1, 20, value), year
    """
    req = SenseiRequest(stmt)
    self.assertEqual(len(req.facets), 3)
    facet_color = req.facets["color"]
    self.assertEqual(facet_color.expand, False)
    self.assertEqual(facet_color.minHits, sensei_client.DEFAULT_FACET_MINHIT)
    self.assertEqual(facet_color.maxCounts, sensei_client.DEFAULT_FACET_MAXHIT)
    self.assertEqual(facet_color.orderBy, sensei_client.PARAM_FACET_ORDER_HITS)
    facet_price = req.facets["price"]
    self.assertEqual(facet_price.expand, True)
    self.assertEqual(facet_price.minHits, 1)
    self.assertEqual(facet_price.maxCounts, 20)
    self.assertEqual(facet_price.orderBy, sensei_client.PARAM_FACET_ORDER_VAL)
    facet_color = req.facets["year"]
    self.assertEqual(facet_color.expand, False)
    self.assertEqual(facet_color.minHits, sensei_client.DEFAULT_FACET_MINHIT)
    self.assertEqual(facet_color.maxCounts, sensei_client.DEFAULT_FACET_MAXHIT)
    self.assertEqual(facet_color.orderBy, sensei_client.PARAM_FACET_ORDER_HITS)

  def testGivenClause(self):
    stmt = \
    """
    SELECT *
    FROM cars
    GIVEN FACET PARAM (My-Network, "srcid", int, 8233570)
    """
    req = SenseiRequest(stmt)
    self.assertEqual(len(req.facet_init_param_map), 1)
    init_params = req.facet_init_param_map["My-Network"]
    self.assertEqual(init_params.int_map["srcid"], [8233570])

  def testGivenClause2(self):
    stmt = \
    """
    SELECT *
    FROM cars
    GIVEN FACET PARAM (My-Network, "srcid", int, 8233570),
                      (time, "now", long, "999999"),   -- Accept string too
                      (member, "last_name", string, "Cui")
    """
    req = SenseiRequest(stmt)
    self.assertEqual(len(req.facet_init_param_map), 3)
    init_params = req.facet_init_param_map["My-Network"]
    self.assertEqual(init_params.int_map["srcid"], [8233570])
    init_params = req.facet_init_param_map["time"]
    self.assertEqual(init_params.long_map["now"], ["999999"])
    init_params = req.facet_init_param_map["member"]
    self.assertEqual(init_params.string_map["last_name"], ["Cui"])

  def testFetchingStored(self):
    stmt = \
    """
    SELECT *
    FROM cars
    FETCHING STORED TRUE
    """
    req = SenseiRequest(stmt)
    self.assertTrue(req.fetch_stored)

    stmt = \
    """
    SELECT *
    FROM cars
    """
    req = SenseiRequest(stmt)
    self.assertTrue(req.fetch_stored)

    stmt = \
    """
    SELECT *
    FROM cars
    FETCHING STORED False
    """
    req = SenseiRequest(stmt)
    self.assertFalse(req.fetch_stored)

    stmt = \
    """
    SELECT *
    FROM cars
    FETCHING STORED False
    FETCHING STORED true
    """
    # Make sure we only allow one FETCHING STORED clause
    intactFlag = True
    try:
      req = SenseiRequest(stmt)
      intactFlag = False
    except ParseException as err:
      pass
    finally:
      self.assertTrue(intactFlag)

  def testBetweenPredicate(self):
    stmt = \
    """
    SELECT *
    FROM cars
    WHERE year BETWEEN 2000 AND 2001
    """
    req = SenseiRequest(stmt)
    self.compare("rows=10&select.year.val=%5B2000+TO+2001%5D" +
                 "&select.year.op=or&start=0&select.year.not=&fetchstored=true",
                 SenseiClient.buildUrlString(req))

    stmt = \
    """
    SELECT *
    FROM cars
    WHERE year NOT BETWEEN 1999 AND 2000
    """
    req = SenseiRequest(stmt)
    self.compare("rows=10&select.year.val=&select.year.op=or" +
                 "&start=0&select.year.not=%5B1999+TO+2000%5D&fetchstored=true",
                 SenseiClient.buildUrlString(req))

  def testNotEqualPredicate(self):
    stmt = \
    """
    SELECT *
    FROM cars
    WHERE color <> "red"
    """
    req = SenseiRequest(stmt)
    self.assertEqual(len(req.selections), 1)
    select_color = req.selections["color"]
    self.assertEqual(select_color.field, "color")
    self.assertEqual(select_color.operation, sensei_client.PARAM_SELECT_OP_OR)
    self.assertEqual(select_color.values, [])
    self.assertEqual(select_color.excludes, ["red"])

  def testSelectionConflict(self):
    stmt = \
    """
    SELECT *
    FROM cars
    WHERE color = "red"
      AND color = "blue"
    """
    error = None
    try:
      req = SenseiRequest(stmt)
    except SenseiClientError as err:
      error = str(err)
    self.assertEqual(error, repr("There is conflict in selection(s) for column 'color'"))

  def testSelectionMerge(self):
    stmt = \
    """
    SELECT *
    FROM cars
    WHERE color <> "red" AND color <> "blue"
    """
    req = SenseiRequest(stmt)
    self.assertEqual(len(req.selections), 1)
    select_color = req.selections["color"]
    self.assertEqual(select_color.field, "color")
    self.assertEqual(select_color.operation, sensei_client.PARAM_SELECT_OP_OR)
    self.assertEqual(select_color.values, [])
    self.assertEqual(len(select_color.excludes), 2)
    self.assertTrue("red" in select_color.excludes)
    self.assertTrue("blue" in select_color.excludes)

    stmt = \
    """
    SELECT *
    FROM cars
    WHERE year NOT BETWEEN 1995 AND 1996
      AND year NOT BETWEEN 1999 AND 2000
    """
    req = SenseiRequest(stmt)
    self.assertEqual(len(req.selections), 1)
    select = req.selections["year"]
    self.assertEqual(select.field, "year")
    self.assertEqual(select.operation, sensei_client.PARAM_SELECT_OP_OR)
    self.assertEqual(select.values, [])
    self.assertEqual(len(select.excludes), 2)
    self.assertTrue("[1995 TO 1996]" in select.excludes)
    self.assertTrue("[1999 TO 2000]" in select.excludes)

  def testCumulativePredicate(self):
    stmt = \
    """
    SELECT *
    FROM cars
    WHERE (color = "red" OR
           color in ("blue", "yellow") OR
           color = "black")
    """
    req = SenseiRequest(stmt)
    self.assertEqual(len(req.selections), 1)
    select_color = req.selections["color"]
    self.assertEqual(select_color.field, "color")
    self.assertEqual(select_color.operation, sensei_client.PARAM_SELECT_OP_OR)
    self.assertEqual(len(select_color.values), 4)
    self.assertTrue("red" in select_color.values)
    self.assertTrue("blue" in select_color.values)
    self.assertTrue("yellow" in select_color.values)
    self.assertTrue("black" in select_color.values)

    stmt = \
    """
    SELECT *
    FROM cars
    WHERE (color = "red" OR
           color in ("blue", "yellow") OR
           color = "black")
      AND (year BETWEEN 1999 AND 2000 OR
           year BETWEEN 1995 AND 1996)
    """
    req = SenseiRequest(stmt)
    self.assertEqual(len(req.selections), 2)
    select_color = req.selections["color"]
    self.assertEqual(select_color.field, "color")
    self.assertEqual(select_color.operation, sensei_client.PARAM_SELECT_OP_OR)
    self.assertEqual(len(select_color.values), 4)
    self.assertTrue("red" in select_color.values)
    self.assertTrue("blue" in select_color.values)
    self.assertTrue("yellow" in select_color.values)
    self.assertTrue("black" in select_color.values)

    select_year = req.selections["year"]
    self.assertEqual(select_year.field, "year")
    self.assertEqual(select_year.operation, sensei_client.PARAM_SELECT_OP_OR)
    self.assertEqual(len(select_year.values), 2)
    self.assertTrue("[1999 TO 2000]" in select_year.values)
    self.assertTrue("[1995 TO 1996]" in select_year.values)

  def testCumulativePredicateOnly(self):
    stmt = \
    """
    SELECT *
    FROM cars
    WHERE color = "red" OR color = "blue"
    """
    req = SenseiRequest(stmt)
    self.assertEqual(len(req.selections), 1)
    select_color = req.selections["color"]
    self.assertEqual(select_color.field, "color")
    self.assertEqual(select_color.operation, sensei_client.PARAM_SELECT_OP_OR)
    self.assertEqual(len(select_color.values), 2)
    self.assertTrue("red" in select_color.values)
    self.assertTrue("blue" in select_color.values)

  def testCumulativePredicateError(self):
    stmt = \
    """
    SELECT *
    FROM cars
    WHERE (color = "red" OR year = 1999)
    """
    error = None
    try:
      req = SenseiRequest(stmt)
    except SenseiClientError as err:
      error = str(err)
    self.assertEqual(error, repr("A different column 'year' appeared in cumulative predicates"))

    stmt = \
    """
    SELECT *
    FROM cars
    WHERE (color = "red" OR
           color NOT in ("blue", "yellow") OR
           color = "black")
    """
    error = None
    try:
      req = SenseiRequest(stmt)
    except SenseiClientError as err:
      error = str(err)
    self.assertEqual(error, repr("Negative predicate for column 'color' appeared in cumulative predicates"))


  def compare(self, paramStr1, paramStr2):
    """Compare two URL param strings built by Sensei client.

    Make sure two strings are equivalent.

    """

    list1 = paramStr1.split('&')
    list1.sort()
    list2 = paramStr2.split('&')
    list2.sort()
    self.assertTrue(len(paramStr1) == len(paramStr2) and list1 == list2)


if __name__ == "__main__":
    unittest.main()

"""
TODO:

1. BETWEEN ... AND ...

   SELECT *
   FROM cars
   WHERE year BETWEEN 1995 AND 1996

   SELECT *
   FROM cars
   WHERE year NOT BETWEEN 1995 AND 1996

   SELECT *
   FROM cars
   WHERE year NOT BETWEEN 1995 AND 1996
     AND 
     AND year NOT BETWEEN 2000 AND 2001

2. Relevance

3. Make sure that we do not have predicate conflict:

   SELECT *
   FROM cars
   WHERE color = "red" AND color = "blue"

   But, the following NOT predicate should be OK:

   WHERE color <> "red" AND color <> "blue"

   WHERE year NOT BETWEEN 1995 AND 1996
     AND year NOT BETWEEN 1995 AND 1996

   WHERE (color = "red" OR color = "blue")
     AND (year BETWEEN 1995 AND 1996 OR
          year BETWEEN 2000 AND 2001)


predicates ::= (predicate + ZeroOrMore(AND + predicate))

predicate ::= in_predicate
            | contains_all_predicate
            | between_predicate
            | negative_predicate
            | "(" or_positive_predicates ")"

positive_predicate ::= in_predicate | contains_all_predicate



"""
