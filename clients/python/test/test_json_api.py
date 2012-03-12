import sys
import unittest
from os.path import dirname

sys.path.insert(0, dirname(__file__) + "/../sensei")
from sensei import BQLRequest, SenseiClientError, SenseiFacet, SenseiSelection,\
                   SenseiSort, SenseiFacetInitParams, SenseiFacetInfo,\
                   SenseiNodeInfo, SenseiSystemInfo, SenseiRequest, SenseiHit,\
                   SenseiResultFacet, SenseiResult, SenseiClient
# from sensei_components import *
from pyparsing import ParseException, ParseFatalException, ParseSyntaxException

sensei_client = SenseiClient(sysinfo={
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


class TestJsonAPI(unittest.TestCase):
  """ Test cases for BQL using JSON API."""

  def testBasics(self):
    stmt = \
    """
    SELECT color, price
    FROM cars
    WHERE color in ("red", "blue");
    """
    req = sensei_client.compile(stmt)
    # print sensei_client.buildJsonString(req, indent=2)
    self.assertEqual(sensei_client.buildJsonString(req, indent=2),
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
    req = sensei_client.compile(stmt)
    # print sensei_client.buildJsonString(req, indent=2)
    self.assertEqual(sensei_client.buildJsonString(req, indent=2),
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
      "path": {
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
    req = sensei_client.compile(stmt)
    # print sensei_client.buildJsonString(req, indent=2)
    self.assertEqual(sensei_client.buildJsonString(req, indent=2),
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

  def testQueryAndSelections1(self):
    stmt = \
    """
    SELECT *
    FROM cars
    WHERE QUERY IS "cool AND moon-roof"
      AND color = "red"
      AND category = "sedan"
    """
    req = sensei_client.compile(stmt)
    # print sensei_client.buildJsonString(req, indent=2)
    self.assertEqual(sensei_client.buildJsonString(req, indent=2),
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
        "category": {
          "value": "sedan"
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
    req = sensei_client.compile(stmt)
    # print sensei_client.buildJsonString(req, indent=2)
    self.assertEqual(sensei_client.buildJsonString(req, indent=2),
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
    req = sensei_client.compile(stmt)
    # print sensei_client.buildJsonString(req, indent=2)
    self.assertEqual(sensei_client.buildJsonString(req, indent=2),
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
    req = sensei_client.compile(stmt)
    self.assertEqual(sensei_client.buildJsonString(req, indent=2),
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
    req = sensei_client.compile(stmt)
    # print sensei_client.buildJsonString(req, indent=2)
    self.assertEqual(sensei_client.buildJsonString(req, indent=2),
                    """{
  "fetchStored": true, 
  "from": 0, 
  "groupBy": {
    "columns": [
      "color"
    ], 
    "top": 10
  }, 
  "size": 10
}""")


  def testRangePred1(self):
    stmt = \
    """
    SELECT *
    FROM cars
    WHERE year > 1999
    """

    req = sensei_client.compile(stmt)
    # print sensei_client.buildJsonString(req, indent=2)
    self.assertEqual(sensei_client.buildJsonString(req, indent=2),
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

  def testRangePred2(self):
    stmt = \
    """
    SELECT *
    FROM cars
    WHERE year > 1999 AND year <= 2003 AND year >= 1999
    """

    req = sensei_client.compile(stmt)
    # print sensei_client.buildJsonString(req, indent=2)
    self.assertEqual(sensei_client.buildJsonString(req, indent=2),
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

  def testRangePred3(self):
    stmt = \
    """
    SELECT *
    FROM cars
    WHERE name > "abc" AND name < "xyz" AND name >= "ddd"
    """

    req = sensei_client.compile(stmt)
    # print sensei_client.buildJsonString(req, indent=2)
    self.assertEqual(sensei_client.buildJsonString(req, indent=2),
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
      req = sensei_client.compile(stmt)
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

    req = sensei_client.compile(stmt)
    # print sensei_client.buildJsonString(req, indent=2)
    self.assertEqual(sensei_client.buildJsonString(req, indent=2),
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
    req = sensei_client.compile(stmt)
    # print sensei_client.buildJsonString(req, indent=2)
    self.assertEqual(sensei_client.buildJsonString(req, indent=2),
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
      req = sensei_client.compile(stmt)
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
    req = sensei_client.compile(stmt)
    # print sensei_client.buildJsonString(req, indent=2)
    self.assertEqual(sensei_client.buildJsonString(req, indent=2),
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
    req = sensei_client.compile(stmt)
    # print sensei_client.buildJsonString(req, indent=2)
    self.assertEqual(sensei_client.buildJsonString(req, indent=2),
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
    req = sensei_client.compile(stmt)
    # print sensei_client.buildJsonString(req, indent=2)
    self.assertEqual(sensei_client.buildJsonString(req, indent=2),
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
    req = sensei_client.compile(stmt)
    # print sensei_client.buildJsonString(req, indent=2)
    self.assertEqual(sensei_client.buildJsonString(req, indent=2),
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
      req = sensei_client.compile(stmt)
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
    req = sensei_client.compile(stmt)
    # print sensei_client.buildJsonString(req, indent=2)
    self.assertEqual(sensei_client.buildJsonString(req, indent=2),
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

  def testContainsAllPred(self):
    stmt = \
    """
    SELECT *
    FROM cars
    WHERE tags CONTAINS ALL ("hybrid", "moon-roof", "leather")
    """
    req = sensei_client.compile(stmt)
    # print sensei_client.buildJsonString(req, indent=2)
    self.assertEqual(sensei_client.buildJsonString(req, indent=2),
                     """{
  "fetchStored": true, 
  "from": 0, 
  "selections": [
    {
      "terms": {
        "tags": {
          "_noOptimize": false, 
          "excludes": [], 
          "operator": "and", 
          "values": [
            "hybrid", 
            "moon-roof", 
            "leather"
          ]
        }
      }
    }
  ], 
  "size": 10
}""")

  def testPathPred1(self):
    stmt = \
    """
    SELECT *
    FROM cars
    WHERE city = "china/hongkong" WITH ("strict":false, "depth":1)
    """
    req = sensei_client.compile(stmt)
    # print sensei_client.buildJsonString(req, indent=2)
    self.assertEqual(sensei_client.buildJsonString(req, indent=2),
                     """{
  "fetchStored": true, 
  "from": 0, 
  "selections": [
    {
      "path": {
        "city": {
          "depth": 1, 
          "strict": false, 
          "value": "china/hongkong"
        }
      }
    }
  ], 
  "size": 10
}""")

  def testPathPred2(self):
    stmt = \
    """
    SELECT *
    FROM cars
    WHERE city = "china/hongkong" WITH ("strict":false, "ddd":1)
    """
    error = None
    try:
      req = sensei_client.compile(stmt)
    except ParseFatalException as err:
      error = err.msg
    # print error
    self.assertEqual(error, 'Property, "ddd", is not supported for facet "city"')


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
#     req = sensei_client.compile(stmt)
#     print sensei_client.buildJsonString(req, indent=2)
#     # self.assertEqual(sensei_client.buildJsonString(req, indent=2),

  def testGroupBy1(self):
    stmt = \
    """
    SELECT *
    FROM cars
    GROUP BY color
    """
    req = sensei_client.compile(stmt)
    # print sensei_client.buildJsonString(req, indent=2)
    self.assertEqual(sensei_client.buildJsonString(req, indent=2),
                     """{
  "fetchStored": true, 
  "from": 0, 
  "groupBy": {
    "columns": [
      "color"
    ], 
    "top": 10
  }, 
  "size": 10
}""")

  def testGroupBy2(self):
    stmt = \
    """
    SELECT *
    FROM cars
    GROUP     BY color top 3
    """
    req = sensei_client.compile(stmt)
    # print sensei_client.buildJsonString(req, indent=2)
    self.assertEqual(sensei_client.buildJsonString(req, indent=2),
                     """{
  "fetchStored": true, 
  "from": 0, 
  "groupBy": {
    "columns": [
      "color"
    ], 
    "top": 3
  }, 
  "size": 10
}""")
    

  def testBrowseBy1(self):
    stmt = \
    """
    SELECT *
    FROM cars
    BROWSE BY color(false, 1, 10, hits), price(true, 1, 20, value)
    """
    req = sensei_client.compile(stmt)
    # print sensei_client.buildJsonString(req, indent=2)
    self.assertEqual(sensei_client.buildJsonString(req, indent=2),
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
    req = sensei_client.compile(stmt)
    # print sensei_client.buildJsonString(req, indent=2)
    self.assertEqual(sensei_client.buildJsonString(req, indent=2),
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
    req = sensei_client.compile(stmt)
    # print sensei_client.buildJsonString(req, indent=2)
    self.assertEqual(sensei_client.buildJsonString(req, indent=2),
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
    req = sensei_client.compile(stmt)
    # print sensei_client.buildJsonString(req, indent=2)
    self.assertEqual(sensei_client.buildJsonString(req, indent=2),
                     """{
  "from": 0, 
  "size": 10
}""")

  def testOrPred(self):
    stmt = \
    """
    SELECT *
    FROM cars
    WHERE color = "red" OR color = "blue"
    """
    req = sensei_client.compile(stmt)
    # print sensei_client.buildJsonString(req, indent=2)
    self.assertEqual(sensei_client.buildJsonString(req, indent=2),
                     """{
  "fetchStored": true, 
  "filter": {
    "or": [
      {
        "term": {
          "color": {
            "value": "red"
          }
        }
      }, 
      {
        "term": {
          "color": {
            "value": "blue"
          }
        }
      }
    ]
  }, 
  "from": 0, 
  "size": 10
}""")

  def testAndOrPred(self):
    stmt = \
    """
    SELECT color, year, tags
    FROM cars
    WHERE (color = "red" or color = "blue")
       OR (color = "black" AND tags contains all ("hybrid", "moon-roof", "leather"))
    GROUP BY color
    """
    req = sensei_client.compile(stmt)
    # print sensei_client.buildJsonString(req, indent=2)
    self.assertEqual(sensei_client.buildJsonString(req, indent=2),
                     """{
  "fetchStored": true, 
  "filter": {
    "or": [
      {
        "or": [
          {
            "term": {
              "color": {
                "value": "red"
              }
            }
          }, 
          {
            "term": {
              "color": {
                "value": "blue"
              }
            }
          }
        ]
      }, 
      {
        "and": [
          {
            "term": {
              "color": {
                "value": "black"
              }
            }
          }, 
          {
            "terms": {
              "tags": {
                "_noOptimize": false, 
                "excludes": [], 
                "operator": "and", 
                "values": [
                  "hybrid", 
                  "moon-roof", 
                  "leather"
                ]
              }
            }
          }
        ]
      }
    ]
  }, 
  "from": 0, 
  "groupBy": {
    "columns": [
      "color"
    ], 
    "top": 10
  }, 
  "size": 10
}""")
    
  def testBetweenPred1(self):
    stmt = \
    """
    SELECT *
    FROM cars
    WHERE year BETWEEN 2000 AND 2001
    """
    req = sensei_client.compile(stmt)
    # print sensei_client.buildJsonString(req, indent=2)
    self.assertEqual(sensei_client.buildJsonString(req, indent=2),
                     """{
  "fetchStored": true, 
  "from": 0, 
  "selections": [
    {
      "range": {
        "year": {
          "from": 2000, 
          "include_lower": true, 
          "include_upper": true, 
          "to": 2001
        }
      }
    }
  ], 
  "size": 10
}""")

  def testBetweenPred2(self):
    stmt = \
    """
    SELECT *
    FROM cars
    WHERE year NOT BETWEEN 2000 AND 2001
    """
    req = sensei_client.compile(stmt)
    # print sensei_client.buildJsonString(req, indent=2)
    self.assertEqual(sensei_client.buildJsonString(req, indent=2),
                     """{
  "fetchStored": true, 
  "filter": {
    "or": [
      {
        "range": {
          "year": {
            "include_upper": false, 
            "to": 2000
          }
        }
      }, 
      {
        "range": {
          "year": {
            "from": 2001, 
            "include_lower": false
          }
        }
      }
    ]
  }, 
  "from": 0, 
  "size": 10
}""")

  def testSelectionConflict(self):
    # XXX To be supported later
    return

    stmt = \
    """
    SELECT *
    FROM cars
    WHERE color = "red"
      AND color = "blue"
    """
    error = None
    try:
      req = sensei_client.compile(stmt)
    except SenseiClientError as err:
      error = str(err)
    self.assertEqual(error, repr("There is conflict in selection(s) for column 'color'"))

  def testSelectionMerge(self):
    # XXX
    return

    stmt = \
    """
    SELECT *
    FROM cars
    WHERE color <> "red" AND color <> "blue"
    """

  def testMultipleQueries(self):
    stmt = \
    """
    SELECT tags, color
      FROM cars
     WHERE (color = "red" AND query is "hybrid AND cool")
        OR (color = "blue" AND query is "moon-roof AND navigation")
    """
    req = sensei_client.compile(stmt)
    # print sensei_client.buildJsonString(req, indent=2),
    self.assertEqual(sensei_client.buildJsonString(req, indent=2),
                     """{
  "fetchStored": true, 
  "filter": {
    "or": [
      {
        "and": [
          {
            "term": {
              "color": {
                "value": "red"
              }
            }
          }, 
          {
            "query": {
              "query_string": {
                "query": "hybrid AND cool"
              }
            }
          }
        ]
      }, 
      {
        "and": [
          {
            "term": {
              "color": {
                "value": "blue"
              }
            }
          }, 
          {
            "query": {
              "query_string": {
                "query": "moon-roof AND navigation"
              }
            }
          }
        ]
      }
    ]
  }, 
  "from": 0, 
  "size": 10
}""")

  def testMatchPred(self):
    stmt = \
    """
    SELECT *
      FROM cars
     WHERE MATCH(f1, f2) AGAINST("text1 AND text2")
    """
    req = sensei_client.compile(stmt)
    # print sensei_client.buildJsonString(req, indent=2),
    self.assertEqual(sensei_client.buildJsonString(req, indent=2),
                     """{
  "fetchStored": true, 
  "from": 0, 
  "query": {
    "query_string": {
      "fields": [
        "f1", 
        "f2"
      ], 
      "query": "text1 AND text2"
    }
  }, 
  "size": 10
}""")

  def testColumnType1(self):
    stmt = \
    """
    SELECT *
    FROM cars
    WHERE color = 1
    """
    error = None
    try:
      req = sensei_client.compile(stmt)
    except ParseFatalException as err:
      error = err.msg
    self.assertEqual(error, """Value, 1, is not of type "string" (for facet "color")""")

  def testColumnType2(self):
    stmt = \
    """
    SELECT *
    FROM cars
    WHERE mileage = 111
       OR (color IN ("red", "blue") AND year > "bbb")
    """
    error = None
    try:
      req = sensei_client.compile(stmt)
    except ParseFatalException as err:
      error = err.msg
    self.assertEqual(error, """Value, "bbb", is not of type "int" (for facet "year")""")

  def testColumnType3(self):
    stmt = \
    """
    SELECT *
    FROM cars
    WHERE color IN ("red", 123)
    """
    error = None
    try:
      req = sensei_client.compile(stmt)
    except ParseFatalException as err:
      error = err.msg
    self.assertEqual(error, """Value, 123, is not of type "string" (for facet "color")""")

  def testColumnType4(self):
    stmt = \
    """
    SELECT *
    FROM cars
    WHERE tags CONTAINS ALL ("cool", 999)
    """
    error = None
    try:
      req = sensei_client.compile(stmt)
    except ParseFatalException as err:
      error = err.msg
    # print error
    self.assertEqual(error, """Value, 999, is not of type "string" (for facet "tags")""")

  def testRangePredCreation(self):
    stmt = \
    """
    SELECT *
    FROM cars
    WHERE year = 1999
    """
    req = sensei_client.compile(stmt)
    # print sensei_client.buildJsonString(req, indent=2),
    self.assertEqual(sensei_client.buildJsonString(req, indent=2),
                     """{
  "fetchStored": true, 
  "from": 0, 
  "selections": [
    {
      "range": {
        "year": {
          "from": 1999, 
          "include_lower": true, 
          "include_upper": true, 
          "to": 1999
        }
      }
    }
  ], 
  "size": 10
}""")

  def testFacetType1(self):
    stmt = \
    """
    SELECT *
    FROM cars
    WHERE color > "red"
    """
    error = None
    try:
      req = sensei_client.compile(stmt)
    except ParseFatalException as err:
      error = err.msg
    # print error
    self.assertEqual(error, """Column, "color", is not range facet""")

  def testFacetType2(self):
    stmt = \
    """
    SELECT *
    FROM cars
    WHERE color BETWEEN "red" AND "yellow"
    """
    error = None
    try:
      req = sensei_client.compile(stmt)
    except ParseFatalException as err:
      error = err.msg
    # print error
    self.assertEqual(error, """Column, "color", is not range facet""")

  def testLikePredicate1(self):
    stmt = \
    """
    SELECT *
    FROM cars
    WHERE category LIKE "s_d%"  -- Notice the SQL syntax
    """
    req = sensei_client.compile(stmt)
    # print sensei_client.buildJsonString(req, indent=2),
    self.assertEqual(sensei_client.buildJsonString(req, indent=2),
                     """{
  "fetchStored": true, 
  "from": 0, 
  "query": {
    "wildcard": {
      "category": "s?d*"
    }
  }, 
  "size": 10
}""")

  def testLikePredicate2(self):
    stmt = \
    """
    SELECT *
    FROM cars
    WHERE MATCH(contents) AGAINST("cool")
      AND category LIKE "sed*"  -- Also accepts Lucene syntax
    """
    req = sensei_client.compile(stmt)
    # print sensei_client.buildJsonString(req, indent=2),
    self.assertEqual(sensei_client.buildJsonString(req, indent=2),
                     """{
  "fetchStored": true, 
  "filter": {
    "query": {
      "wildcard": {
        "category": "sed*"
      }
    }
  }, 
  "from": 0, 
  "query": {
    "query_string": {
      "fields": [
        "contents"
      ], 
      "query": "cool"
    }
  }, 
  "size": 10
}""")

  def testLikePredicate3(self):
    stmt = \
    """
    SELECT *
    FROM cars
    WHERE price LIKE "123%"
    """
    error = None
    try:
      req = sensei_client.compile(stmt)
    except ParseFatalException as err:
      error = err.msg
    # print error
    self.assertEqual(error, """Column, "price", is not a string type""")

  def testQueryAndLike(self):
    stmt = \
    """
    SELECT color, category, tags
    FROM cars
    WHERE color LIKE "bl%"
      AND MATCH(contents) AGAINST("cool AND moon-roof")
      AND category LIKE "%an"
    """
    req = sensei_client.compile(stmt)
    # print sensei_client.buildJsonString(req, indent=2),
    self.assertEqual(sensei_client.buildJsonString(req, indent=2),
                     """{
  "fetchStored": true, 
  "filter": {
    "and": [
      {
        "query": {
          "query_string": {
            "fields": [
              "contents"
            ], 
            "query": "cool AND moon-roof"
          }
        }
      }, 
      {
        "query": {
          "wildcard": {
            "category": "*an"
          }
        }
      }
    ]
  }, 
  "from": 0, 
  "query": {
    "wildcard": {
      "color": "bl*"
    }
  }, 
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
    req = sensei_client.compile(stmt)
    print sensei_client.buildJsonString(req, indent=2),
    # self.assertEqual(sensei_client.buildJsonString(req, indent=2),
    """...
    """
'''
