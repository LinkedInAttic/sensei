import random
import unittest

from sensei_client_lib import *

class TestSenseiClientLib(unittest.TestCase):

  def setUp(self):
    self.proxy = SenseiServiceProxy()

  def test_QueryMatchAllWithBoost(self):
    req = SenseiRequest()
    req.set_query(SenseiQueryMatchAll().set_boost(1.2))
    sensei_results = self.proxy.doQuery(req)
    self.assertEqual(15000, sensei_results.numHits, "result is not correct for matchall query")
  
  def test_QueryString(self):
    req = SenseiRequest()
    req.set_query(SenseiQueryString("red AND cool"))
    sensei_results = self.proxy.doQuery(req)
    self.assertEqual(1070, sensei_results.numHits, "result is not correct for query string")
    
  def test_QueryTerm(self):
    req = SenseiRequest()
    req.set_query(SenseiQueryTerm("color", "red"))
    sensei_results = self.proxy.doQuery(req)
    self.assertEqual(2160, sensei_results.numHits, "result is not correct for term query")
    
  def test_QueryText(self):  
    pass
    
    
  def test_selectionTerm(self):
    req = SenseiRequest()
    req.append_selection(SenseiSelectionTerm("color", "red"))
    sensei_results = self.proxy.doQuery(req)
    self.assertEqual(2160, sensei_results.numHits, "result is not correct for term selection")
    
  def test_selectionRange1(self):
    req = SenseiRequest()
    req.append_selection(SenseiSelectionRange("year", from_str="2000", to_str="2002", include_lower=True, include_upper=True))
    sensei_results = self.proxy.doQuery(req)
    self.assertEqual(4455, sensei_results.numHits, "result is not correct for range selection 1")
    
  def test_selectionRange2(self):
    req = SenseiRequest()
    req.append_selection(SenseiSelectionRange("year", from_str="2000", to_str="2002", include_lower=False, include_upper=True))
    sensei_results = self.proxy.doQuery(req)
    self.assertEqual(2907, sensei_results.numHits, "result is not correct for range selection 2")        
   
  def test_selectionRange3(self):
    req = SenseiRequest()
    req.append_selection(SenseiSelectionRange("year", from_str="2000", to_str="2002", include_lower=False, include_upper=False))
    sensei_results = self.proxy.doQuery(req)
    self.assertEqual(1443, sensei_results.numHits, "result is not correct for range selection 3")     
        
    
  def test_filterUIDs(self):
    req = SenseiRequest()
    req.set_filter(SenseiFilterIDs([1,2,3], [2]))
    sensei_results = self.proxy.doQuery(req)
    self.assertEqual(2, sensei_results.numHits, "result is not correct for UID filter")  
    
  def test_filterTerm(self):
    req = SenseiRequest()
    req.set_filter(SenseiFilterTerm("color", "red"))
    sensei_results = self.proxy.doQuery(req)
    self.assertEqual(2160, sensei_results.numHits, "result is not correct for term filter")  

  def test_filterTerms(self):
    req = SenseiRequest()
    req.set_filter(SenseiFilterTerms("tags", values=["leather","moon-roof"], excludes=["hybrid"]))
    sensei_results = self.proxy.doQuery(req)
    self.assertEqual(5777, sensei_results.numHits, "result is not correct for terms filter")  
    
  def test_filterRange(self):
    req = SenseiRequest()
    req.set_filter(SenseiFilterRange("year", 1999, 2000))
    sensei_results = self.proxy.doQuery(req)
    self.assertEqual(3015, sensei_results.numHits, "result is not correct for range filter")     
    
  def test_filterAnd(self):   
    req = SenseiRequest()
    req.set_filter(SenseiFilterAND([SenseiFilterTerm("tags","mp3"), SenseiFilterTerm("color","red")]))
    sensei_results = self.proxy.doQuery(req)
    self.assertEqual(439, sensei_results.numHits, "result is not correct for and filter")     

  def test_filterOr(self):   
    req = SenseiRequest()
    req.set_filter(SenseiFilterOR([SenseiFilterTerm("color","blue"), SenseiFilterTerm("color","red")]))
    sensei_results = self.proxy.doQuery(req)
    self.assertEqual(3264, sensei_results.numHits, "result is not correct for and filter")   
    
  def test_filterBool(self):   
    req = SenseiRequest()
    req.set_filter(SenseiFilterBool(must_filter=SenseiFilterTerm("color","red"), must_not_filter=SenseiFilterTerm("category","compact"), should_filter=[SenseiFilterTerm("color","red"), SenseiFilterTerm("color","green")]))
    sensei_results = self.proxy.doQuery(req)
    self.assertEqual(1652, sensei_results.numHits, "result is not correct for and filter")   
 
             
if __name__ == '__main__':
  unittest.main()