package com.senseidb.search.req.mapred;

import junit.framework.TestCase;

import org.apache.log4j.Logger;
import org.json.JSONObject;

import com.senseidb.svc.api.SenseiService;
import com.senseidb.test.SenseiStarter;
import com.senseidb.test.TestSensei;

public class TestMapReduce extends TestCase {

    private static final Logger logger = Logger.getLogger(TestMapReduce.class);

    
    private static SenseiService httpRestSenseiService;
    static {
      SenseiStarter.start("test-conf/node1","test-conf/node2");     
      httpRestSenseiService = SenseiStarter.httpRestSenseiService;
    }
    
    
    
    public void test2GroupByColorAndGroupId() throws Exception { 
      String req = "{\"size\":0,\"filter\":{\"terms\":{\"color\":{\"includes\":[],\"excludes\":[\"gold\"],\"operator\":\"or\"}}}" +
          ", \"mapReduce\":{\"function\":\"com.senseidb.search.req.mapred.CountGroupByMapReduce\",\"parameters\":{\"columns\":[\"groupid\", \"color\"]}}}";
      JSONObject reqJson = new JSONObject(req);
      System.out.println(reqJson.toString(1));
      JSONObject res = TestSensei.search(reqJson);
    
      JSONObject highestResult = res.getJSONObject("mapReduceResult").getJSONArray("groupedCounts").getJSONObject(0);
      assertEquals(8, highestResult.getInt(highestResult.keys().next().toString()));
    }
  
   
   
    public void test4MaxMapReduce() throws Exception {      
      String req = "{\"filter\":{\"term\":{\"color\":\"red\"}}"
          +", \"mapReduce\":{\"function\":\"sensei.max\",\"parameters\":{\"column\":\"groupid\"}}}";
      JSONObject res = TestSensei.search(new JSONObject(req));
      JSONObject mapReduceResult = res.getJSONObject("mapReduceResult");
      assertEquals(14990, Long.parseLong(mapReduceResult.getString("max")));
      assertEquals(14994, Long.parseLong(mapReduceResult.getString("uid")));
       req = "{\"filter\":{\"term\":{\"color\":\"red\"}}"
          +  ",\"selections\":[{\"terms\":{\"groupid\":{\"excludes\":[14990],\"operator\":\"or\"}}}]"
          + ", \"mapReduce\":{\"function\":\"sensei.max\",\"parameters\":{\"column\":\"groupid\"}}}";
       res = TestSensei.search(new JSONObject(req));
       mapReduceResult = res.getJSONObject("mapReduceResult");
      assertEquals(14980, Long.parseLong(mapReduceResult.getString("max")));
      //assertEquals(14989, Long.parseLong(mapReduceResult.getString("uid")));
      
    }
    public void test5DistinctCount() throws Exception {      
      String req = "{\"filter\":{\"term\":{\"color\":\"red\"}}"
          +", \"mapReduce\":{\"function\":\"sensei.distinctCount\",\"parameters\":{\"column\":\"groupid\"}}}";
      JSONObject res = TestSensei.search(new JSONObject(req));
      JSONObject mapReduceResult = res.getJSONObject("mapReduceResult");
      assertEquals(964, Long.parseLong(mapReduceResult.getString("distinctCount")));
     
       req = "{"
          +" \"mapReduce\":{\"function\":\"sensei.distinctCountHashSet\",\"parameters\":{\"column\":\"groupid\"}}}";
       res = TestSensei.search(new JSONObject(req));
       mapReduceResult = res.getJSONObject("mapReduceResult");
      assertEquals(1509, Long.parseLong(mapReduceResult.getString("distinctCount")));
    }
    public void test6MinMapReduce() throws Exception {      
      String req = "{\"filter\":{\"term\":{\"tags\":\"reliable\"}}"
          +", \"mapReduce\":{\"function\":\"sensei.min\",\"parameters\":{\"column\":\"groupid\"}}}";
      JSONObject res = TestSensei.search(new JSONObject(req));
      JSONObject mapReduceResult = res.getJSONObject("mapReduceResult");
      assertEquals(-15000L, Long.parseLong(mapReduceResult.getString("min")));
      assertEquals(0L, Long.parseLong(mapReduceResult.getString("uid")));
       req = "{\"filter\":{\"term\":{\"tags\":\"reliable\"}}"
          +", \"mapReduce\":{\"function\":\"sensei.min\",\"parameters\":{\"column\":\"year\"}}}";
       res = TestSensei.search(new JSONObject(req));
       mapReduceResult = res.getJSONObject("mapReduceResult");
      assertEquals(1993L, Long.parseLong(mapReduceResult.getString("min")));
    }
    public void test7SumMapReduce() throws Exception {      
      String req = "{\"filter\":{\"term\":{\"color\":\"red\"}}, "
          +" \"mapReduce\":{\"function\":\"sensei.sum\",\"parameters\":{\"column\":\"groupid\"}}}";
      JSONObject res = TestSensei.search(new JSONObject(req));
      JSONObject mapReduceResult = res.getJSONObject("mapReduceResult");
      assertEquals(16036500, mapReduceResult.getLong("sum"));
    }
    public void test8AvgMapReduce() throws Exception {      
      String req = "{\"filter\":{\"term\":{\"color\":\"red\"}}, "
          +" \"mapReduce\":{\"function\":\"sensei.avg\",\"parameters\":{\"column\":\"groupid\"}}}";
      JSONObject res = TestSensei.search(new JSONObject(req));
      JSONObject mapReduceResult = res.getJSONObject("mapReduceResult");
      assertEquals(7424, mapReduceResult.getLong("avg"));
      assertEquals(2160, Long.parseLong(mapReduceResult.getString("count")));
    }
    public void test9FacetCountMapReduce() throws Exception {      
      String req = "{\"facets\": {\"color\": {\"max\": 10, \"minCount\": 1, \"expand\": false, \"order\": \"hits\"}}"
          +", \"mapReduce\":{\"function\":\"com.senseidb.search.req.mapred.FacetCountsMapReduce\",\"parameters\":{\"column\":\"color\"}}}";
      JSONObject res = TestSensei.search(new JSONObject(req));
      JSONObject mapReduceResult = res.getJSONObject("mapReduceResult");
      System.out.println(mapReduceResult.toString(1));
      assertEquals(3141, mapReduceResult.getJSONObject("facetCounts").getInt("black"));
      assertEquals(2196, mapReduceResult.getJSONObject("facetCounts").getInt("white"));
      
      
    }
    public void test10FacetCountMapReduceWithFilter() throws Exception {      
      String req = "{\"facets\": {\"color\": {\"max\": 10, \"minCount\": 1, \"expand\": false, \"order\": \"hits\"}}"
          +", \"mapReduce\":{\"function\":\"com.senseidb.search.req.mapred.FacetCountsMapReduce\",\"parameters\":{\"column\":\"color\"}}, " +
          "\"filter\":{\"term\":{\"tags\":\"reliable\"}}}";
      JSONObject res = TestSensei.search(new JSONObject(req));
      JSONObject mapReduceResult = res.getJSONObject("mapReduceResult");
      System.out.println(mapReduceResult.toString(1));
      assertEquals(2259, mapReduceResult.getJSONObject("facetCounts").getInt("black"));
      assertEquals(1560, mapReduceResult.getJSONObject("facetCounts").getInt("white"));
      
      
    }
}
