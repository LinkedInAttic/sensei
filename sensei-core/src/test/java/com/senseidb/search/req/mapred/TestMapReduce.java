package com.senseidb.search.req.mapred;

import junit.framework.TestCase;

import org.apache.log4j.Logger;

import com.senseidb.search.req.mapred.impl.MapReduceBroker;
import com.senseidb.search.req.mapred.impl.MapReduceRequest;
import com.senseidb.search.req.mapred.impl.MapReduceResult;
import com.senseidb.svc.api.SenseiService;
import com.senseidb.test.SenseiStarter;

public class TestMapReduce extends TestCase {

    private static final Logger logger = Logger.getLogger(TestMapReduce.class);

    private static MapReduceBroker mapReduceBroker;
    private static SenseiService httpRestSenseiService;
    static {
      SenseiStarter.start("test-conf/node1","test-conf/node2");
      mapReduceBroker = SenseiStarter.mapReduceBroker;
      httpRestSenseiService = SenseiStarter.httpRestSenseiService;
    }
    public void test1Max() throws Exception {
      MapReduceResult result = mapReduceBroker.browse(new MapReduceRequest(new MaxMapReduce("groupid")));
      assertEquals(14990L, (long)(Long)result.getReduceResult());
      System.out.println("!!!" + result.getTime());
    }
    public void test2GroupByColorAndGroupId() throws Exception {
      MapReduceResult result = mapReduceBroker.browse(new MapReduceRequest(new CountGroupByMapReduce("color", "groupid")));
      System.out.println("!!!" + result.getReduceResult());
      System.out.println("!!!" + result.getTime());
    }
    public void test3GroupByColorAndGroupId() throws Exception {      
      MapReduceResult result = mapReduceBroker.browse(new MapReduceRequest(new CountGroupByMapReduce("color", "groupid")));
      System.out.println("!!!" + result.getReduceResult());
      System.out.println("!!!" + result.getTime());
    }
}
