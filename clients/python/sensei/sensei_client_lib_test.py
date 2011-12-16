import random
import unittest

from sensei_client_lib import *

class TestSenseiClientLib(unittest.TestCase):

  def setUp(self):
    self.proxy = SenseiServiceProxy()

#    self.assertEqual(1, 1, "true")
#    self.assertRaises(TypeError, random.shuffle, (1,2,3))
#    self.assertTrue(element in self.seq)
    
  def test_QueryTerm(self):
    req = SenseiRequest()
    req.set_query(SenseiQueryTerm("color", "red"))
    sensei_results = self.proxy.doQuery(req)
    self.assertEqual(2160, sensei_results.numHits, "result is not correct for term query")
    

if __name__ == '__main__':
  unittest.main()