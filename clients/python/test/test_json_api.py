import sys
import unittest
import time
from datetime import datetime

sys.path.insert(0, "../src")
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
    # print SenseiClient.buildJsonString(req, indent=2),
    self.assertEqual(SenseiClient.buildJsonString(req, indent=2),
                    """{
  "fetchStored": true, 
  "filters": {
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
  "from": 0, 
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
    # print SenseiClient.buildJsonString(req, indent=2),
    self.assertEqual(SenseiClient.buildJsonString(req, indent=2),
                     """{
  "fetchStored": true, 
  "filters": {
    "and": [
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
          "make": "honda"
        }
      }
    ]
  }, 
  "from": 0, 
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
    # print SenseiClient.buildJsonString(req, indent=2),
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

  def testQueryAndFilter(self):
    stmt = \
    """
    SELECT *
    FROM cars
    WHERE QUERY IS "cool AND moon-roof"
      AND color = "red"
      AND year = 1995
    """
    req = SenseiRequest(stmt)
    # print SenseiClient.buildJsonString(req, indent=2),
    self.assertEqual(SenseiClient.buildJsonString(req, indent=2),
                     """{
  "fetchStored": true, 
  "filters": {
    "and": [
      {
        "term": {
          "color": "red"
        }
      }, 
      {
        "term": {
          "year": 1995
        }
      }
    ]
  }, 
  "from": 0, 
  "query": {
    "query_string": {
      "query": "cool AND moon-roof"
    }
  }, 
  "size": 10
}""")

  def testQueryAndFilter2(self):
    stmt = \
    """
    SELECT *
    FROM cars
    WHERE QUERY IS "cool AND moon-roof"
      AND color = "red"
    """
    req = SenseiRequest(stmt)
    # print SenseiClient.buildJsonString(req, indent=2),
    self.assertEqual(SenseiClient.buildJsonString(req, indent=2),
                     """{
  "fetchStored": true, 
  "filters": {
    "term": {
      "color": "red"
    }
  }, 
  "from": 0, 
  "query": {
    "query_string": {
      "query": "cool AND moon-roof"
    }
  }, 
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


  def testRangePredicates(self):
    stmt = \
    """
    SELECT *
    FROM cars
    WHERE year > 1999
    """

    req = SenseiRequest(stmt)
    # print SenseiClient.buildJsonString(req, indent=2),
    self.assertEqual(SenseiClient.buildJsonString(req, indent=2),
                     """{
  "fetchStored": true, 
  "filters": {
    "range": {
      "year": {
        "from": 1999, 
        "include_lower": false, 
        "include_upper": false, 
        "to": "*"
      }
    }
  }, 
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
    req = SenseiRequest(stmt)
    # print SenseiClient.buildJsonString(req, indent=2),
    self.assertEqual(SenseiClient.buildJsonString(req, indent=2),
    """"""
'''
