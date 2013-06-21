/**
 * This software is licensed to you under the Apache License, Version 2.0 (the
 * "Apache License").
 *
 * LinkedIn's contributions are made under the Apache License. If you contribute
 * to the Software, the contributions will be deemed to have been made under the
 * Apache License, unless you expressly indicate otherwise. Please do not make any
 * contributions that would be inconsistent with the Apache License.
 *
 * You may obtain a copy of the Apache License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, this software
 * distributed under the Apache License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the Apache
 * License for the specific language governing permissions and limitations for the
 * software governed under the Apache License.
 *
 * © 2012 LinkedIn Corp. All Rights Reserved.  
 */
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
    JSONObject substituted = (JSONObject) jsonTemplateProcessor.process(requestJson, jsonTemplateProcessor.getTemplates(requestJson));
    System.out.println(substituted.toString(1));
    assertEquals(10, substituted.getInt("count"));
    assertEquals(1.0, substituted.getDouble("boost"), 0.01);
    assertEquals("prefix_$substitutedParam_$$substitutedParam_suffix$$routeParam", substituted.getString("routeParam"));

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
 


  

}
