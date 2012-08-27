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

package com.senseidb.test.plugin;

import java.util.List;

import junit.framework.Assert;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.lucene.analysis.Analyzer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.browseengine.bobo.facets.FacetHandler;
import com.browseengine.bobo.facets.impl.SimpleFacetHandler;
import com.browseengine.bobo.facets.impl.VirtualSimpleFacetHandler;
import com.senseidb.plugin.SenseiPluginRegistry;

public class SenseiConfigurationTest extends Assert {
  private PropertiesConfiguration configuration;
  private SenseiPluginRegistry pluginRegistry;
  @Before
  public void setUp() throws Exception {
    
    configuration = new PropertiesConfiguration();
    configuration.setDelimiterParsingDisabled(true);
    //configuration.setListDelimiter(';');
    configuration.load(getClass().getClassLoader().getResource("refactored-configuration/sensei.properties"));
    pluginRegistry = SenseiPluginRegistry.build(configuration);
    pluginRegistry.start();
    

  }
  @After
  public void tearDown() {
    pluginRegistry.stop();
  }
  @Test
  public void test1GetBeanByFullPrefix() {
   
    Analyzer analyzer = pluginRegistry.getBeanByFullPrefix("sensei.index.analyzer", Analyzer.class);
    assertNotNull(analyzer);
   
  }
  @Test
  public void test2GetBeanByName() {
    Analyzer analyzer = pluginRegistry.getBeanByName("analyzer", Analyzer.class);
    assertNotNull(analyzer);
  }
  @Test
  public void test3ConfigParams() {
    MyCustomRouterFactory customRouterFactory = pluginRegistry.getBeansByType(MyCustomRouterFactory.class).get(0);
    assertEquals("prop1", customRouterFactory.config.get("property1"));
    assertEquals("prop2", customRouterFactory.config.get("property2"));
    assertEquals("3", customRouterFactory.config.get("property3"));
    assertEquals("", customRouterFactory.config.get("property4"));
    assertTrue(customRouterFactory.started);
  }
  @Test
  public void test4GetBeanList() {
   List<FacetHandler> customFacets = pluginRegistry.resolveBeansByListKey("sensei.custom.facets", FacetHandler.class);
   assertEquals(6, customFacets.size());
   assertTrue(customFacets.get(0) instanceof VirtualSimpleFacetHandler);
   assertTrue(customFacets.get(4) instanceof SimpleFacetHandler);
  }
  @Test
  public void test5GetEmptyBeanList() {
   List<Object> beans = pluginRegistry.resolveBeansByListKey("sensei.plugin.services", Object.class);
   assertEquals(0, beans.size());
  }
  @Test
  public void test6GetFacet() {

   assertEquals("virtual_groupid", pluginRegistry.getFacet("virtual_groupid").getName());
   assertEquals("virtual_groupid_fixedlengthlongarray", pluginRegistry.getFacet("virtual_groupid_fixedlengthlongarray").getName());

  }
  @Test
  public void test7GetRuntimeFacet() {

   assertEquals("mockHandlerFactory", pluginRegistry.getRuntimeFacet("mockHandlerFactory").getName());
   assertEquals("virtual_groupid_fixedlengthlongarray", pluginRegistry.getFacet("virtual_groupid_fixedlengthlongarray").getName());

  }
  
  public void test8GetRuntimeFacetClassCast() {

   assertNull(pluginRegistry.getFacet("mockHandlerFactory"));
   

  }
}
