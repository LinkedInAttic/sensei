package com.sensei.plugin;

import java.util.List;

import junit.framework.Assert;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.lucene.analysis.Analyzer;
import org.junit.Before;
import org.junit.Test;

import com.browseengine.bobo.facets.FacetHandler;
import com.browseengine.bobo.facets.impl.SimpleFacetHandler;
import com.browseengine.bobo.facets.impl.VirtualSimpleFacetHandler;
import com.sensei.plugin.test.MyCustomRouterFactory;

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
    assertTrue(customRouterFactory.started);
  }
  @Test
  public void test4GetBeanList() {
   List<FacetHandler> customFacets = pluginRegistry.resolveBeansByListKey("sensei.custom.facets", FacetHandler.class);
   assertEquals(5, customFacets.size());
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
}
