import sys
import unittest

sys.path.insert(0, "../src")
import sensei_client
from sensei_client import SenseiRequest

class TestSenseiClient(unittest.TestCase):

  def setUp(self):
    pass

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
    select = req.selections[0]
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
    select = req.selections[0]
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
    select = req.selections[0]
    self.assertEqual(select.field, "age")
    self.assertEqual(select.operation, sensei_client.PARAM_SELECT_OP_OR)
    self.assertEqual(select.values, [20, 30, "40"])
    select = req.selections[1]
    self.assertEqual(select.field, "last_name")
    self.assertEqual(select.operation, sensei_client.PARAM_SELECT_OP_AND)
    self.assertEqual(select.values, ["Cui"])

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
    self.assertEqual(len(req.selections), 3)
    select_color = req.selections[0]
    self.assertEqual(select_color.field, "color")
    self.assertEqual(select_color.operation, sensei_client.PARAM_SELECT_OP_OR)
    self.assertEqual(select_color.values, ["gold", "green", "blue"])
    self.assertEqual(select_color.excludes, ["black"])
    select_year = req.selections[1]
    self.assertEqual(select_year.field, "year")
    self.assertEqual(select_year.operation, sensei_client.PARAM_SELECT_OP_OR)
    self.assertEqual(select_year.values, ["[1996 TO 1997]", "[2002 TO 2003]"])
    self.assertEqual(select_year.properties["aaa"], "111")
    self.assertEqual(select_year.properties["bbb"], "222")
    select_tags = req.selections[2]
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
    GROUP BY color top 3
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

if __name__ == "__main__":
    unittest.main()
