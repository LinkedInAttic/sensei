package com.sensei.test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

import org.apache.lucene.search.SortField;
import org.json.JSONObject;
import org.junit.BeforeClass;
import org.junit.Test;

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
    
    //test facet initial parameters;
    
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

}
