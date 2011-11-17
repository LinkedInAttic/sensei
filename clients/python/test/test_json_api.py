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
    self.assertTrue(SenseiClient.buildJsonString(req, indent=2),
                    """{
  "fetchStored": "true", 
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
    self.assertTrue(SenseiClient.buildJsonString(req, indent=2),
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
  "fetchStored": "true", 
  "from": 0, 
  "size": 10
}""")

if __name__ == "__main__":
    unittest.main()
