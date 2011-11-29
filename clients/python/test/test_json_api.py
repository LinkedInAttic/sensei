import sys
import unittest
import time
from datetime import datetime

sys.path.insert(0, "../sensei")
import sensei_client
from sensei_client import *
from pyparsing import ParseException


class TestJsonAPI(unittest.TestCase):
  """ Test cases for BQL using JSON API."""

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
    # print SenseiClient.buildJsonString(req, indent=2)
    self.assertEqual(SenseiClient.buildJsonString(req, indent=2),
                    """{
  "fetchStored": true, 
  "from": 0, 
  "selections": [
    {
      "terms": {
        "color": {
          "_noOptimize": false, 
          "excludes": [], 
          "operator": "or", 
          "values": [
            "red", 
            "blue"
          ]
        }
      }
    }
  ], 
  "size": 10
}""")

  def testSimpleAnd(self):
    stmt = \
    """
    SELECT color, price
    FROM cars
    WHERE color in ("red", "blue")
      AND make = "honda"
    """
    req = SenseiRequest(stmt)
    # print SenseiClient.buildJsonString(req, indent=2)
    self.assertEqual(SenseiClient.buildJsonString(req, indent=2),
                     """{
  "fetchStored": true, 
  "from": 0, 
  "selections": [
    {
      "terms": {
        "color": {
          "_noOptimize": false, 
          "excludes": [], 
          "operator": "or", 
          "values": [
            "red", 
            "blue"
          ]
        }
      }
    }, 
    {
      "term": {
        "make": {
          "value": "honda"
        }
      }
    }
  ], 
  "size": 10
}""")

  def testQuery(self):
    stmt = \
    """
    SELECT *
    FROM cars
    WHERE QUERY IS "cool AND moon-roof"
    """
    req = SenseiRequest(stmt)
    # print SenseiClient.buildJsonString(req, indent=2)
    self.assertEqual(SenseiClient.buildJsonString(req, indent=2),
                    """{
  "fetchStored": true, 
  "from": 0, 
  "query": {
    "query_string": {
      "query": "cool AND moon-roof"
    }
  }, 
  "size": 10
}""")

  def testQueryAndSelections(self):
    stmt = \
    """
    SELECT *
    FROM cars
    WHERE QUERY IS "cool AND moon-roof"
      AND color = "red"
      AND year = 1995
    """
    req = SenseiRequest(stmt)
    # print SenseiClient.buildJsonString(req, indent=2)
    self.assertEqual(SenseiClient.buildJsonString(req, indent=2),
                     """{
  "fetchStored": true, 
  "from": 0, 
  "query": {
    "query_string": {
      "query": "cool AND moon-roof"
    }
  }, 
  "selections": [
    {
      "term": {
        "color": {
          "value": "red"
        }
      }
    }, 
    {
      "term": {
        "year": {
          "value": 1995
        }
      }
    }
  ], 
  "size": 10
}""")

  def testQueryAndSelections2(self):
    stmt = \
    """
    SELECT *
    FROM cars
    WHERE QUERY IS "cool AND moon-roof"
      AND color = "red"
    """
    req = SenseiRequest(stmt)
    # print SenseiClient.buildJsonString(req, indent=2)
    self.assertEqual(SenseiClient.buildJsonString(req, indent=2),
                     """{
  "fetchStored": true, 
  "from": 0, 
  "query": {
    "query_string": {
      "query": "cool AND moon-roof"
    }
  }, 
  "selections": [
    {
      "term": {
        "color": {
          "value": "red"
        }
      }
    }
  ], 
  "size": 10
}""")

  def testBrowseBy(self):
    stmt = \
    """
    SELECT *
    FROM cars
    BROWSE BY color, price(true, 1, 20, value), year
    """
    req = SenseiRequest(stmt)
    # print SenseiClient.buildJsonString(req, indent=2)
    self.assertEqual(SenseiClient.buildJsonString(req, indent=2),
                    """{
  "facets": {
    "color": {
      "expand": false, 
      "max": 10, 
      "minhit": 1, 
      "order": "hits"
    }, 
    "price": {
      "expand": true, 
      "max": 20, 
      "minhit": 1, 
      "order": "val"
    }, 
    "year": {
      "expand": false, 
      "max": 10, 
      "minhit": 1, 
      "order": "hits"
    }
  }, 
  "fetchStored": true, 
  "from": 0, 
  "size": 10
}""")


  def testFacetInitParams(self):
    stmt = \
    """
    SELECT *
    FROM cars
    GIVEN FACET PARAM (My-Network, "srcid", int, 8233570),
                      (time, "now", long, "999999"),   -- Accept string too
                      (member, "last_name", string, "Cui"),
                      (member, "age", int, 25)
    """
    req = SenseiRequest(stmt)
    self.assertEqual(SenseiClient.buildJsonString(req, indent=2),
                    """{
  "facetInit": {
    "My-Network": {
      "srcid": {
        "type": "int", 
        "values": [
          "8233570"
        ]
      }
    }, 
    "member": {
      "age": {
        "type": "int", 
        "values": [
          "25"
        ]
      }, 
      "last_name": {
        "type": "string", 
        "values": [
          "Cui"
        ]
      }
    }, 
    "time": {
      "now": {
        "type": "long", 
        "values": [
          "999999"
        ]
      }
    }
  }, 
  "fetchStored": true, 
  "from": 0, 
  "size": 10
}""")

  def testGroupBy(self):
    stmt = \
    """
    SELECT *
    FROM cars
    GROUP BY color
    """
    req = SenseiRequest(stmt)
    # print SenseiClient.buildJsonString(req, indent=2)
    self.assertEqual(SenseiClient.buildJsonString(req, indent=2),
                    """{
  "fetchStored": true, 
  "from": 0, 
  "groupBy": [
    {
      "column": "color", 
      "top": 10
    }
  ], 
  "size": 10
}""")


  def testRangePredicate1(self):
    stmt = \
    """
    SELECT *
    FROM cars
    WHERE year > 1999
    """

    req = SenseiRequest(stmt)
    # print SenseiClient.buildJsonString(req, indent=2)
    self.assertEqual(SenseiClient.buildJsonString(req, indent=2),
                     """{
  "fetchStored": true, 
  "from": 0, 
  "selections": [
    {
      "range": {
        "year": {
          "from": 1999, 
          "include_lower": false
        }
      }
    }
  ], 
  "size": 10
}""")

  def testRangePredicate2(self):
    stmt = \
    """
    SELECT *
    FROM cars
    WHERE year > 1999 AND year <= 2003 AND year >= 1999
    """

    req = SenseiRequest(stmt)
    # print SenseiClient.buildJsonString(req, indent=2)
    self.assertEqual(SenseiClient.buildJsonString(req, indent=2),
                     """{
  "fetchStored": true, 
  "from": 0, 
  "selections": [
    {
      "range": {
        "year": {
          "from": 1999, 
          "include_lower": false, 
          "include_upper": true, 
          "to": 2003
        }
      }
    }
  ], 
  "size": 10
}""")

  def testRangePredicate3(self):
    stmt = \
    """
    SELECT *
    FROM cars
    WHERE name > "abc" AND name < "xyz" AND name >= "ddd"
    """

    req = SenseiRequest(stmt)
    # print SenseiClient.buildJsonString(req, indent=2)
    self.assertEqual(SenseiClient.buildJsonString(req, indent=2),
                     """{
  "fetchStored": true, 
  "from": 0, 
  "selections": [
    {
      "range": {
        "name": {
          "from": "ddd", 
          "include_lower": true, 
          "include_upper": false, 
          "to": "xyz"
        }
      }
    }
  ], 
  "size": 10
}""")

  def testRangeConflict(self):
    stmt = \
    """
    SELECT *
    FROM cars
    WHERE year > 1999 AND year < 1995
    """
    error = None
    try:
      req = SenseiRequest(stmt)
    except ParseSyntaxException as err:
      error = str(err)
    self.assertTrue(error != None)

  def testFilterAndSelection(self):
    stmt = \
    """
    SELECT *
    FROM cars
    WHERE color IN ("red", "blue")  -- a selection
      AND fff = 1234                -- a filter
    """

    req = SenseiRequest(stmt, facet_list=['color'])
    # print SenseiClient.buildJsonString(req, indent=2)
    self.assertEqual(SenseiClient.buildJsonString(req, indent=2),
                     """{
  "fetchStored": true, 
  "filter": {
    "term": {
      "fff": {
        "value": 1234
      }
    }
  }, 
  "from": 0, 
  "selections": [
    {
      "terms": {
        "color": {
          "_noOptimize": false, 
          "excludes": [], 
          "operator": "or", 
          "values": [
            "red", 
            "blue"
          ]
        }
      }
    }
  ], 
  "size": 10
}""")


  def testNotEqual(self):
    stmt = \
    """
    SELECT color, price
    FROM cars
    WHERE color <> "red"
    """
    req = SenseiRequest(stmt)
    # print SenseiClient.buildJsonString(req, indent=2)
    self.assertEqual(SenseiClient.buildJsonString(req, indent=2),
                     """{
  "fetchStored": true, 
  "from": 0, 
  "selections": [
    {
      "terms": {
        "color": {
          "_noOptimize": false, 
          "excludes": [
            "red"
          ], 
          "operator": "or", 
          "values": []
        }
      }
    }
  ], 
  "size": 10
}""")

  def testKeyword(self):
    stmt = \
    """
    SELECT *
    FROM cars
    WHERE NOT IN ("red", "blue")  -- 'NOT' here cannot be treated as column name
    """
    intactFlag = True
    try:
      req = SenseiRequest(stmt)
      intactFlag = False
    except ParseException as err:
      pass
    finally:
      self.assertTrue(intactFlag)

  def testNotIn(self):
    stmt = \
    """
    SELECT color, price
    FROM cars
    WHERE color NOT IN ("yellow", "green");
    """
    req = SenseiRequest(stmt)
    # print SenseiClient.buildJsonString(req, indent=2)
    self.assertEqual(SenseiClient.buildJsonString(req, indent=2),
                     """{
  "fetchStored": true, 
  "from": 0, 
  "selections": [
    {
      "terms": {
        "color": {
          "_noOptimize": false, 
          "excludes": [
            "yellow", 
            "green"
          ], 
          "operator": "or", 
          "values": []
        }
      }
    }
  ], 
  "size": 10
}""")

  def testLiterals(self):
    stmt = \
    """
    SELECT *
    FROM people
    WHERE age in (20, 30, "40")         -- Now we accept both string and integer
      AND last_name = "Cui"
    """
    req = SenseiRequest(stmt)
    # print SenseiClient.buildJsonString(req, indent=2),
    self.assertEqual(SenseiClient.buildJsonString(req, indent=2),
                     """{
  "fetchStored": true, 
  "from": 0, 
  "selections": [
    {
      "terms": {
        "age": {
          "_noOptimize": false, 
          "excludes": [], 
          "operator": "or", 
          "values": [
            20, 
            30, 
            "40"
          ]
        }
      }
    }, 
    {
      "term": {
        "last_name": {
          "value": "Cui"
        }
      }
    }
  ], 
  "size": 10
}""")

  def testOrderBy(self):
    stmt = \
    """
    SELECT *
    FROM cars
    ORDER BY year desc, price
    """
    req = SenseiRequest(stmt)
    # print SenseiClient.buildJsonString(req, indent=2),
    self.assertEqual(SenseiClient.buildJsonString(req, indent=2),
                     """{
  "fetchStored": true, 
  "from": 0, 
  "size": 10, 
  "sort": [
    {
      "year": "desc"
    }, 
    {
      "price": "asc"
    }
  ]
}""")

  def testOrderByRelevance(self):
    stmt = \
    """
    SELECT *
    FROM cars
    ORDER BY relevance
    """
    req = SenseiRequest(stmt)
    # print SenseiClient.buildJsonString(req, indent=2),
    self.assertEqual(SenseiClient.buildJsonString(req, indent=2),
                     """{
  "fetchStored": true, 
  "from": 0, 
  "size": 10, 
  "sort": [
    "_score"
  ]
}""")

  def testOrderByRelevanceDesc(self):
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
    except ParseSyntaxException as err:
      pass
    finally:
      self.assertTrue(intactFlag)

  def testInPred(self):
    stmt = \
    """
    SELECT *
    FROM cars
    WHERE color IN ("red", "blue") EXCEPT("black")
    """
    req = SenseiRequest(stmt)
    # print SenseiClient.buildJsonString(req, indent=2),
    self.assertEqual(SenseiClient.buildJsonString(req, indent=2),
                     """{
  "fetchStored": true, 
  "from": 0, 
  "selections": [
    {
      "terms": {
        "color": {
          "_noOptimize": false, 
          "excludes": [
            "black"
          ], 
          "operator": "or", 
          "values": [
            "red", 
            "blue"
          ]
        }
      }
    }
  ], 
  "size": 10
}""")

#   def testWhereConditions(self):
#     stmt = \
#     """
#     SELECT color, year, tags, price
#     FROM cars
#     WHERE query is "cool"
#       AND color in ("gold", "green", "blue") EXCEPT ("black")
#       AND year in ("[1996 TO 1997]", "[2002 TO 2003]") WITH ("aaa":"111", "bbb":"222")
#       and tags contains all ("hybrid", "favorite")
#     ORDER BY price desc
#     LIMIT 5, 20
#     """
#     req = SenseiRequest(stmt)
#     print SenseiClient.buildJsonString(req, indent=2),
#     # self.assertEqual(SenseiClient.buildJsonString(req, indent=2),

if __name__ == "__main__":
    unittest.main()

'''
  def testQuery(self):
    stmt = \
    """
    SELECT *
    FROM cars
    WHERE QUERY IS "cool AND moon-roof"
    """
    req = SenseiRequest(stmt)
    print SenseiClient.buildJsonString(req, indent=2),
    # self.assertEqual(SenseiClient.buildJsonString(req, indent=2),
    """...
    """
'''
