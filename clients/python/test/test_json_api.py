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
    self.client = SenseiClient(sysinfo={
        "lastmodified": 0, 
        "facets": [
          {
            "runtime": False, 
            "name": "tags", 
            "props": {
              "column": "tags", 
              "depends": "[]", 
              "type": "multi", 
              "column_type": "string"
              }
            }, 
          {
            "runtime": False, 
            "name": "mileage", 
            "props": {
              "column": "mileage", 
              "range": "[*-12500, 12501-15000, 15001-17500, 17501-*]", 
              "type": "range", 
              "depends": "[]", 
              "column_type": "int"
              }
            }, 
          {
            "runtime": False, 
            "name": "category", 
            "props": {
              "column": "category", 
              "depends": "[]", 
              "type": "simple", 
              "column_type": "string"
              }
            }, 
          {
            "runtime": False, 
            "name": "groupid", 
            "props": {
              "column": "groupid", 
              "depends": "[]", 
              "type": "simple", 
              "column_type": "long"
              }
            }, 
          {
            "runtime": False, 
            "name": "year", 
            "props": {
              "column": "year", 
              "range": "[1993-1994, 1995-1996, 1997-1998, 1999-2000, 2001-2002]", 
              "type": "range", 
              "depends": "[]", 
              "column_type": "int"
              }
            }, 
          {
            "runtime": False, 
            "name": "city", 
            "props": {
              "column": "city", 
              "separator": "[/]", 
              "depends": "[]", 
              "type": "path", 
              "column_type": "string"
              }
            }, 
          {
            "runtime": False, 
            "name": "price", 
            "props": {
              "column": "price", 
              "range": "[*,6700, 6800,9900, 10000,13100, 13200,17300, 17400,*]", 
              "type": "range", 
              "depends": "[]", 
              "column_type": "float"
              }
            }, 
          {
            "runtime": False, 
            "name": "makemodel", 
            "props": {
              "column": "makemodel", 
              "depends": "[]", 
              "type": "path", 
              "column_type": "string"
              }
            }, 
          {
            "runtime": False, 
            "name": "color", 
            "props": {
              "column": "color", 
              "depends": "[]", 
              "type": "simple", 
              "column_type": "string"
              }
            }
          ], 
        "version": "4957216", 
        "clusterinfo": [
          {
            "adminlink": "http://192.168.1.104:8080", 
            "nodelink": "192.168.1.104:1234", 
            "id": 1, 
            "partitions": [
              0, 
              1
              ]
            }
          ], 
        "numdocs": 15000
        })

  def testBasics(self):
    stmt = \
    """
    SELECT color, price
    FROM cars
    WHERE color in ("red", "blue");
    """
    req = self.client.compile(stmt)
    # print self.client.buildJsonString(req, indent=2)
    self.assertEqual(self.client.buildJsonString(req, indent=2),
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
      AND makemodel = "honda"
    """
    req = self.client.compile(stmt)
    # print self.client.buildJsonString(req, indent=2)
    self.assertEqual(self.client.buildJsonString(req, indent=2),
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
        "makemodel": {
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
    req = self.client.compile(stmt)
    # print self.client.buildJsonString(req, indent=2)
    self.assertEqual(self.client.buildJsonString(req, indent=2),
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
    req = self.client.compile(stmt)
    # print self.client.buildJsonString(req, indent=2)
    self.assertEqual(self.client.buildJsonString(req, indent=2),
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
    req = self.client.compile(stmt)
    # print self.client.buildJsonString(req, indent=2)
    self.assertEqual(self.client.buildJsonString(req, indent=2),
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
    req = self.client.compile(stmt)
    # print self.client.buildJsonString(req, indent=2)
    self.assertEqual(self.client.buildJsonString(req, indent=2),
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
    req = self.client.compile(stmt)
    self.assertEqual(self.client.buildJsonString(req, indent=2),
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
    req = self.client.compile(stmt)
    # print self.client.buildJsonString(req, indent=2)
    self.assertEqual(self.client.buildJsonString(req, indent=2),
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

    req = self.client.compile(stmt)
    # print self.client.buildJsonString(req, indent=2)
    self.assertEqual(self.client.buildJsonString(req, indent=2),
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

    req = self.client.compile(stmt)
    # print self.client.buildJsonString(req, indent=2)
    self.assertEqual(self.client.buildJsonString(req, indent=2),
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

    req = self.client.compile(stmt)
    # print self.client.buildJsonString(req, indent=2)
    self.assertEqual(self.client.buildJsonString(req, indent=2),
                     """{
  "fetchStored": true, 
  "filter": {
    "range": {
      "name": {
        "from": "ddd", 
        "include_lower": true, 
        "include_upper": false, 
        "to": "xyz"
      }
    }
  }, 
  "from": 0, 
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
      req = self.client.compile(stmt)
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

    req = self.client.compile(stmt)
    # print self.client.buildJsonString(req, indent=2)
    self.assertEqual(self.client.buildJsonString(req, indent=2),
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
    req = self.client.compile(stmt)
    # print self.client.buildJsonString(req, indent=2)
    self.assertEqual(self.client.buildJsonString(req, indent=2),
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
      req = self.client.compile(stmt)
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
    req = self.client.compile(stmt)
    # print self.client.buildJsonString(req, indent=2)
    self.assertEqual(self.client.buildJsonString(req, indent=2),
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
    req = self.client.compile(stmt)
    # print self.client.buildJsonString(req, indent=2)
    self.assertEqual(self.client.buildJsonString(req, indent=2),
                     """{
  "fetchStored": true, 
  "filter": {
    "and": [
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
    ]
  }, 
  "from": 0, 
  "size": 10
}""")

  def testOrderBy(self):
    stmt = \
    """
    SELECT *
    FROM cars
    ORDER BY year desc, price
    """
    req = self.client.compile(stmt)
    # print self.client.buildJsonString(req, indent=2)
    self.assertEqual(self.client.buildJsonString(req, indent=2),
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
    req = self.client.compile(stmt)
    # print self.client.buildJsonString(req, indent=2)
    self.assertEqual(self.client.buildJsonString(req, indent=2),
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
      req = self.client.compile(stmt)
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
    req = self.client.compile(stmt)
    # print self.client.buildJsonString(req, indent=2)
    self.assertEqual(self.client.buildJsonString(req, indent=2),
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

  def testPathPred(self):
    stmt = \
    """
    SELECT *
    FROM cars
    WHERE city IN ("china/hongkong") WITH ("strict":false, "depth":1)
    """
    req = self.client.compile(stmt)
    # XXX
    print self.client.buildJsonString(req, indent=2)
    # self.assertEqual(self.client.buildJsonString(req, indent=2),
    


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
#     req = self.client.compile(stmt)
#     print self.client.buildJsonString(req, indent=2)
#     # self.assertEqual(self.client.buildJsonString(req, indent=2),

  def testGroupBy1(self):
    stmt = \
    """
    SELECT *
    FROM cars
    GROUP BY color
    """
    req = self.client.compile(stmt)
    # print self.client.buildJsonString(req, indent=2)
    self.assertEqual(self.client.buildJsonString(req, indent=2),
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

  def testGroupBy2(self):
    stmt = \
    """
    SELECT *
    FROM cars
    GROUP     BY color top 3
    """
    req = self.client.compile(stmt)
    # print self.client.buildJsonString(req, indent=2)
    self.assertEqual(self.client.buildJsonString(req, indent=2),
                     """{
  "fetchStored": true, 
  "from": 0, 
  "groupBy": [
    {
      "column": "color", 
      "top": 3
    }
  ], 
  "size": 10
}""")
    

  def testBrowseBy1(self):
    stmt = \
    """
    SELECT *
    FROM cars
    BROWSE BY color(false, 1, 10, hits), price(true, 1, 20, value)
    """
    req = self.client.compile(stmt)
    # print self.client.buildJsonString(req, indent=2)
    self.assertEqual(self.client.buildJsonString(req, indent=2),
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
    }
  }, 
  "fetchStored": true, 
  "from": 0, 
  "size": 10
}""")

  def testBrowseBy2(self):
    stmt = \
    """
    SELECT *
    FROM cars
    BROWSE BY color, price(true, 1, 20, value), year
    """
    req = self.client.compile(stmt)
    # print self.client.buildJsonString(req, indent=2)
    self.assertEqual(self.client.buildJsonString(req, indent=2),
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

  def testGivenClause1(self):
    stmt = \
    """
    SELECT *
    FROM cars
    GIVEN FACET PARAM (My-Network, "srcid", int, 8233570)
    """
    req = self.client.compile(stmt)
    # print self.client.buildJsonString(req, indent=2)
    self.assertEqual(self.client.buildJsonString(req, indent=2),
                     """{
  "facetInit": {
    "My-Network": {
      "srcid": {
        "type": "int", 
        "values": [
          "8233570"
        ]
      }
    }
  }, 
  "fetchStored": true, 
  "from": 0, 
  "size": 10
}""")

  def testGivenClause2(self):
    # XXX
    stmt = \
    """
    SELECT *
    FROM cars
    GIVEN FACET PARAM (My-Network, "srcid", int, 8233570),
                      (time, "now", long, "999999"),   -- Accept string too
                      (member, "last_name", string, "Cui"),
                      (member, "age", int, 25)
    """

  def testFetchingStored(self):
    stmt = \
    """
    SELECT *
    FROM cars
    FETCHING STORED FALSE
    """
    req = self.client.compile(stmt)
    # print self.client.buildJsonString(req, indent=2)
    self.assertEqual(self.client.buildJsonString(req, indent=2),
                     """{
  "from": 0, 
  "size": 10
}""")


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
    req = self.client.compile(stmt)
    print self.client.buildJsonString(req, indent=2),
    # self.assertEqual(self.client.buildJsonString(req, indent=2),
    """...
    """
'''
