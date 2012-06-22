package com.senseidb.test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;

import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Ignore;

import com.senseidb.search.req.ErrorType;
import com.senseidb.search.req.mapred.CombinerStage;
import com.senseidb.search.req.mapred.FacetCountAccessor;
import com.senseidb.search.req.mapred.FieldAccessor;
import com.senseidb.search.req.mapred.SenseiMapReduce;
import com.senseidb.search.req.mapred.TestMapReduce;
import com.senseidb.svc.api.SenseiService;

public class ErrorHandlingTest extends TestCase {

  private static final Logger logger = Logger.getLogger(TestMapReduce.class);
  public static class MapReduceAdapter implements SenseiMapReduce<Serializable, Serializable> {   
    public void init(JSONObject params) {}
    public Serializable map(int[] docIds, int docIdCount, long[] uids, FieldAccessor accessor, FacetCountAccessor facetCountAccessor) {return new ArrayList();}
    public List<Serializable> combine(List<Serializable> mapResults, CombinerStage combinerStage) {return new ArrayList();}
    public Serializable reduce(List<Serializable> combineResults) {return new ArrayList();}
    public JSONObject render(Serializable reduceResult) {return new JSONObject();}    
  }
  public static class test1JsonError extends MapReduceAdapter {
    @Override
    public void init(JSONObject params) {
      throw new RuntimeException("JsonException", new JSONException("JsonException"));
    }
  }
  public static class test2BoboError extends MapReduceAdapter {
   @Override
  public Serializable map(int[] docIds, int docIdCount, long[] uids, FieldAccessor accessor, FacetCountAccessor facetCountAccessor) {
    throw new RuntimeException("Map exception");
  }
  }
  public static class test3PartitionLevelError extends MapReduceAdapter {
    @Override
    public List<Serializable> combine(List<Serializable> mapResults, CombinerStage combinerStage) {
      if (combinerStage == CombinerStage.partitionLevel) {
        throw new RuntimeException("partition combiner exception");
      }
      return super.combine(mapResults, combinerStage);
    }
   }
  public static class test4NodeLevelError extends MapReduceAdapter {
    @Override
    public List<Serializable> combine(List<Serializable> mapResults, CombinerStage combinerStage) {
      if (combinerStage == CombinerStage.nodeLevel) {
        throw new RuntimeException("node combiner exception");
      }
      return super.combine(mapResults, combinerStage);
    }
   }
  public static class test5BrokerLevelError extends MapReduceAdapter {
    @Override
    public Serializable reduce(List<Serializable> combineResults) {
      throw new RuntimeException("The exception on broker level");
    }
   }
  public static class test6NonSerializableError extends MapReduceAdapter {
   public static class NonSerializable implements Serializable {
     private Object obj = new Object();
   }
    @Override
    public List<Serializable> combine(List<Serializable> mapResults, CombinerStage combinerStage) {
    
    return new ArrayList(java.util.Arrays.asList(new NonSerializable()));
  }}
    public static class test7ResponseJsonError extends MapReduceAdapter {
    @Override
    public JSONObject render(Serializable reduceResult) {
      throw new RuntimeException(new JSONException("renderError"));
    }
   }
  private static SenseiService httpRestSenseiService;
  static {
    SenseiStarter.start("test-conf/node1","test-conf/node2");     
    httpRestSenseiService = SenseiStarter.httpRestSenseiService;
  }
  public void test1ExceptionOInitLevel() throws Exception {
    String req = "{ \"mapReduce\":{\"function\":\"" + test1JsonError.class.getName() + "\"}}";    
    
    JSONObject reqJson = new JSONObject(req);
    System.out.println(reqJson.toString(1));
    JSONObject res = TestSensei.search(reqJson);
    assertEquals(ErrorType.JsonParsingError.getDefaultErrorCode(), res.getInt("errorCode"));
    assertResponseContainsErrors(res, ErrorType.JsonParsingError);
  }
  public void test2BoboError() throws Exception {
    String req = "{ \"mapReduce\":{\"function\":\"" + test2BoboError.class.getName() + "\"}}";    
    
    JSONObject reqJson = new JSONObject(req);
    System.out.println(reqJson.toString(1));
    JSONObject res = TestSensei.search(reqJson);
    assertEquals(ErrorType.BoboExecutionError.getDefaultErrorCode(), res.getInt("errorCode"));
    assertResponseContainsErrors(res, ErrorType.BoboExecutionError, ErrorType.BoboExecutionError, ErrorType.BoboExecutionError);
  }
  public void test3PartitionLevelError() throws Exception {
    String req = "{ \"mapReduce\":{\"function\":\"" + test3PartitionLevelError.class.getName() + "\"}}";    
    
    JSONObject reqJson = new JSONObject(req);
    System.out.println(reqJson.toString(1));
    JSONObject res = TestSensei.search(reqJson);
    assertEquals(ErrorType.BoboExecutionError.getDefaultErrorCode(), res.getInt("errorCode"));
    assertResponseContainsErrors(res, ErrorType.BoboExecutionError, ErrorType.BoboExecutionError, ErrorType.BoboExecutionError);
  }
  public void test4NodeLevelError() throws Exception {
    String req = "{ \"mapReduce\":{\"function\":\"" + test4NodeLevelError.class.getName() + "\"}}";    
    
    JSONObject reqJson = new JSONObject(req);
    System.out.println(reqJson.toString(1));
    JSONObject res = TestSensei.search(reqJson);
    assertEquals(ErrorType.MergePartitionError.getDefaultErrorCode(), res.getInt("errorCode"));
    assertResponseContainsErrors(res, ErrorType.MergePartitionError, ErrorType.MergePartitionError);
  }
  public void test5BrokerLevelError() throws Exception {
    String req = "{ \"mapReduce\":{\"function\":\"" + test5BrokerLevelError.class.getName() + "\"}}";    
    
    JSONObject reqJson = new JSONObject(req);
    System.out.println(reqJson.toString(1));
    JSONObject res = TestSensei.search(reqJson);
    assertEquals(ErrorType.BrokerGatherError.getDefaultErrorCode(), res.getInt("errorCode"));
    assertResponseContainsErrors(res, ErrorType.BrokerGatherError);
  }
  @Ignore
  public void ntest6NonSerializableError() throws Exception {
    String req = "{ \"mapReduce\":{\"function\":\"" + test6NonSerializableError.class.getName() + "\"}}";    
    
    JSONObject reqJson = new JSONObject(req);
    System.out.println(reqJson.toString(1));
    JSONObject res = TestSensei.search(reqJson);
    assertEquals(ErrorType.BrokerGatherError.getDefaultErrorCode(), res.getInt("errorCode"));
    assertResponseContainsErrors(res, ErrorType.BrokerGatherError);
  }
  public void test7ResponseJsonError() throws Exception {
    String req = "{ \"mapReduce\":{\"function\":\"" + test7ResponseJsonError.class.getName() + "\"}}";    
    
    JSONObject reqJson = new JSONObject(req);
    System.out.println(reqJson.toString(1));
    JSONObject res = TestSensei.search(reqJson);
    assertEquals(ErrorType.JsonParsingError.getDefaultErrorCode(), res.getInt("errorCode"));
    assertResponseContainsErrors(res, ErrorType.JsonParsingError);
  }
  public void test8BQLError() throws Exception {
   String req = "{\"bql\":\"select1 * from cars\"}";
    
    JSONObject reqJson = new JSONObject(req);
    System.out.println(reqJson.toString(1));
    JSONObject res = TestSensei.search(reqJson);
    assertEquals(ErrorType.BQLParsingError.getDefaultErrorCode(), res.getInt("errorCode"));
    assertResponseContainsErrors(res, ErrorType.BQLParsingError);
  }
  private void assertResponseContainsErrors(JSONObject res, ErrorType... jsonParsingErrors) throws JSONException {
    for (int i = 0; i < jsonParsingErrors.length; i++) {
      assertEquals(jsonParsingErrors[i].name(), res.getJSONArray("errors").getJSONObject(i).get("errorType"));
    }
    assertEquals(jsonParsingErrors.length, res.getJSONArray("errors").length());
  }

  
    

}
