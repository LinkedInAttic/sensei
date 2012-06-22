package com.senseidb.test;

import junit.framework.TestCase;

import org.apache.log4j.Logger;
import org.json.JSONObject;

import com.senseidb.search.req.mapred.TestMapReduce;
import com.senseidb.svc.api.SenseiService;

public class TestIndexSelector extends TestCase {

  private static final Logger logger = Logger.getLogger(TestIndexSelector.class);

  
  private static SenseiService httpRestSenseiService;
  static {
    SenseiStarter.start("test-conf/node1","test-conf/node2");     
    httpRestSenseiService = SenseiStarter.httpRestSenseiService;
  }
  public void test1SelectionRange1() throws Exception
  {
   
      logger.info("executing test case Selection range [2000 TO 2002]");
      String req = "{\"selections\":[{\"range\":{\"groupid_range\":{\"from\":\"14400\"}}}]}";
      JSONObject res = TestSensei.search(new JSONObject(req));
      assertEquals("numhits is wrong", 600, res.getInt("numhits"));
    }
  public void testSelectionRange2() throws Exception
  {
   
      logger.info("executing test case Selection range [2000 TO 2002]");
      String req = "{\"selections\":[{\"range\":{\"groupid_range\":{\"to\":\"10\"}}}]}";
      JSONObject res = TestSensei.search(new JSONObject(req));
      assertEquals("numhits is wrong", 20, res.getInt("numhits"));
    }
  public void testSelectionRange3() throws Exception
  {
   
      logger.info("executing test case Selection range [2000 TO 2002]");
      String req = "{\"selections\":[{\"range\":{\"groupid_range\":{\"from\":\"0\", \"to\":\"10\"}}}]}";
      JSONObject res = TestSensei.search(new JSONObject(req));
      assertEquals("numhits is wrong", 11, res.getInt("numhits"));
    }
}

