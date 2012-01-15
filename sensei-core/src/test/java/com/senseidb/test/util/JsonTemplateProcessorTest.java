package com.senseidb.test.util;

import junit.framework.TestCase;

import org.apache.commons.io.IOUtils;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;

import com.senseidb.util.JsonTemplateProcessor;



public class JsonTemplateProcessorTest extends TestCase {

  private String senseiRequestStr;
  private JsonTemplateProcessor jsonTemplateProcessor;
  @Override
  @Before
  public void setUp() throws Exception {
     senseiRequestStr = new String(IOUtils.toString(getClass().getClassLoader().getResourceAsStream("json/sensei-request-with-templates.json")));
     jsonTemplateProcessor = new JsonTemplateProcessor();
  }
  @Test
  public void testSubstituteTemplates() throws Exception{
    JSONObject requestJson = new JSONObject(senseiRequestStr);
    System.out.println(requestJson.toString(1));
    JSONObject substituted = jsonTemplateProcessor.substituteTemplates(requestJson);
    assertEquals(10, substituted.getInt("count"));
    assertEquals(1.0, substituted.getDouble("boost"), 0.01);
    assertEquals("substitutedParam", substituted.getString("routeParam"));

  }
  @Test
  public void testSubstituteTemplatesNoMatch() throws Exception{
    JSONObject requestJson = new JSONObject(senseiRequestStr);
    System.out.println(requestJson.toString(1));
    requestJson.remove("templateMapping");
    requestJson.put("templateMapping", new JSONObject().put("unexisitngValue", "value"));
    JSONObject substituted = jsonTemplateProcessor.substituteTemplates(requestJson);
   assertSame(substituted, requestJson);

  }
  @Test
  public void testSubstituteTemplatesWithSpecialCharacters() throws Exception{
    JSONObject requestJson = new JSONObject(senseiRequestStr);
    System.out.println(requestJson.toString(1));
    requestJson.remove("templateMapping");
    requestJson.put("templateMapping", new JSONObject().put("routeParam", "hav\n a quote \" \t "));
    try{
      JSONObject substituted = jsonTemplateProcessor.substituteTemplates(requestJson);
      fail("The IllegalArgumentException should be thrown");
    } catch (IllegalArgumentException ex) {

    }


  }
  public static void main(String[] args) throws Exception {
    String senseiRequest = new String(IOUtils.toString(JsonTemplateProcessorTest.class.getClassLoader().getResourceAsStream("json/sensei-request-with-templates.json")));
    JsonTemplateProcessor templateProcessor = new JsonTemplateProcessor();
    for(int i = 0; i < 10000; i++) {
      JSONObject jsonObject = new JSONObject(senseiRequest);
      templateProcessor.substituteTemplates(jsonObject);
    }
    long timeStamp = System.currentTimeMillis();
    for(int i = 0; i < 10000; i++) {
      JSONObject jsonObject = new JSONObject(senseiRequest);
      //templateProcessor.substituteTemplates(jsonObject);
    }
    System.out.println(System.currentTimeMillis() - timeStamp);

  }
}
