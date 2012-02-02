package com.senseidb.gateway.test;

import java.io.File;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.json.JSONObject;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import proj.zoie.impl.indexing.StreamDataProvider;

import com.senseidb.gateway.SenseiGateway;
import com.senseidb.plugin.SenseiPluginRegistry;

public class TestKafkaGateway {
  static File confFile = new File("src/test/resources/configs/kafka-gateway.properties");

  static SenseiGateway gateway;
  static SenseiPluginRegistry pluginRegistry;

  static Configuration config = null;
  
  @BeforeClass
  public static void init() throws Exception{
    config = new PropertiesConfiguration(confFile);
    pluginRegistry = SenseiPluginRegistry.build(config);
    pluginRegistry.start();

    gateway = pluginRegistry.getBeanByFullPrefix("sensei.gateway", SenseiGateway.class);
  }
  
  @AfterClass
  public static void shutdown() {
    pluginRegistry.stop();
  }
  
  @Test
  public void testSimpleKafka() throws Exception{
    final StreamDataProvider<JSONObject> dataProvider = gateway.buildDataProvider(null, String.valueOf("0"), null, null);
    BaseGatewayTestUtil.doTest(dataProvider);
  }
}
