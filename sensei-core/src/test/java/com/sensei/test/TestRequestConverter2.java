package com.sensei.test;

import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.codec.binary.Base64;
import org.apache.lucene.search.SortField;
import org.json.JSONObject;
import org.junit.BeforeClass;
import org.junit.Test;

import com.browseengine.bobo.api.FacetSpec.FacetSortSpec;
import com.browseengine.bobo.facets.FacetHandlerInitializerParam;
import com.sensei.search.req.SenseiRequest;
import com.sensei.search.util.RequestConverter2;

public class TestRequestConverter2
{

  static JSONObject testJson = null;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception
  {
    File file = new File("clients/javascript/sensei-request.json");
    BufferedReader br = new BufferedReader(new FileReader(file));
    StringBuffer sb = new StringBuffer();
    String line = br.readLine();
    while (line != null)
    {
      if (!line.trim().startsWith("//"))
      {
        if(line.indexOf("//")>0)
          line = line.substring(0, line.indexOf("//"));
        sb.append(line.trim());
      }
      line = br.readLine();
    }
    
    testJson = new JSONObject(sb.toString());

  }

  @Test
  public void test() throws Exception
  {
    SenseiRequest req = RequestConverter2.fromJSON(testJson);
    
    //testquery;
//    assertTrue("query_string is not equal", req.getQuery().toString().equals("this AND that OR thus"));
    
    //test paging;
    assertTrue("offset is not correct", req.getOffset() == 0);
    assertTrue("size is not correct", req.getCount() ==10);
    
    //test group by;
    assertTrue("group by is not correct", req.getGroupBy().equals("category"));
    assertTrue("max per group is not correct", req.getMaxPerGroup() == 3);
    
    //test filters;
    
    
    //test facets;
    assertTrue("facet number is not correct", req.getFacetSpecCount() ==1);
    assertTrue("facet category max", req.getFacetSpec("category").getMaxCount()==10);
    assertTrue("facet category min", req.getFacetSpec("category").getMinHitCount()==1);
    assertTrue("facet category expand", req.getFacetSpec("category").isExpandSelection() == false);
    assertTrue("facet category order", req.getFacetSpec("category").getOrderBy()== FacetSortSpec.OrderHitsDesc);
    
    //test facet initial parameters;
    Map<String, FacetHandlerInitializerParam> mapParams = req.getFacetHandlerInitParamMap();
    assertTrue("facet number is not correct", mapParams.size() ==1);
    FacetHandlerInitializerParam param = mapParams.get("network");
    
    boolean[] coldstart = param.getBooleanParam("coldStart");
    evaluateBool(coldstart, new boolean[]{true});
    
    List<String> names = param.getStringParam("names");
    ArrayList<String> ar = new ArrayList<String>();
    ar.add("a");
    ar.add("b");
    ar.add("c");
    evaluateListString(names, ar);
    
    double[] timeout = param.getDoubleParam("timeOut");
    evaluateDouble(timeout, new double[]{2.4});
    
    int[] srcId = param.getIntParam("srcId");
    evaluateInt(srcId, new int[]{1234});
    
    long[] longId = param.getLongParam("longId");
    evaluateLong(longId, new long[]{1234567890L, 9876543210L});
    
    byte[] base64 = param.getByteArrayParam("binary");
    evaluateBytes(base64, (new String("Hello world")).getBytes());
    
    //test sortby;
    assertTrue("first sort by is not correct", req.getSort()[0].getField().equals("color"));
    assertTrue("first sort by order is not correct", req.getSort()[0].getReverse() == true);
    assertTrue("secondary sort by is not correct", req.getSort()[1] == SortField.FIELD_SCORE);
    
    //test fetchStored;
    assertTrue("fetchStored is not correct", req.isFetchStoredFields() == false);
    
    //test fetchTermVectors;
//    assertTrue("fetchTermVectors is not correct", req.getTermVectorsToFetch().size() ==0);
    
    //test partitions;
    assertTrue("partition test 1", req.getPartitions().contains(1));
    assertTrue("partition test 2", req.getPartitions().contains(2));
    assertTrue("partition size test", req.getPartitions().size() ==2);
    
    //test explain;
    assertTrue("explain test", req.isShowExplanation() == false);
    
    //test routeParam;
    assertTrue("routing parameter test", req.getRouteParam()!= null); // when it is a null, we get a rand int;
  }

  private void evaluateBytes(byte[] result, byte[] sample)
  {
    assertTrue("byte array size is not correct", result.length == sample.length);
    for(int i=0; i< result.length; i++)
    {
      assertTrue("content in byte array is not the same", result[i] == sample[i]);
    }
  }

  private void evaluateLong(long[] result, long[] sample)
  {
    assertTrue("long array size is not correct", result.length == sample.length);
    for(int i=0; i< result.length; i++)
    {
      assertTrue("content in long array is not the same", result[i] == sample[i]);
    }
  }

  private void evaluateInt(int[] result, int[] sample)
  {
    assertTrue("int array size is not correct", result.length == sample.length);
    for(int i=0; i< result.length; i++)
    {
      assertTrue("content in int array is not the same", result[i] == sample[i]);
    }
    
  }

  private void evaluateDouble(double[] result, double[] sample)
  {
    assertTrue("double array size is not correct", result.length == sample.length);
    for(int i=0; i< result.length; i++)
    {
      assertTrue("content in double array is not the same", result[i] == sample[i]);
    }
  }

  private void evaluateListString(List<String> result, ArrayList<String> sample)
  {
    assertTrue("string array size is not correct", result.size() == sample.size());
    for(int i=0; i< result.size(); i++)
    {
      assertTrue("content in double array is not the same", result.get(i).equals(sample.get(i)));
    }
  }

  private void evaluateBool(boolean[] result, boolean[] sample)
  {
    assertTrue("boolean array size is not correct", result.length == sample.length);
    for(int i=0; i< result.length; i++)
    {
      assertTrue("content in boolean array is not the same", result[i] == sample[i]);
    }
  }

  @Test
  public void testBase64() throws Exception
  {
    try {
      String clearText = "Hello world";
      String encodedText;

      // Base64
      encodedText = new String(Base64.encodeBase64(clearText.getBytes()));
//      System.out.println("Encoded: " + encodedText);
      byte[] encodedbytes = encodedText.getBytes();
//      System.out.println("Decoded:"  + new String(Base64.decodeBase64(encodedbytes)));
      //    
      // output :
      //   Encoded: SGVsbG8gd29ybGQ=
      //   Decoded:Hello world      
      //
      assertTrue("Hello world".equals(new String(Base64.decodeBase64(encodedbytes))));
    } 
    catch (Exception e) {
      e.printStackTrace();
    }
  }
}
