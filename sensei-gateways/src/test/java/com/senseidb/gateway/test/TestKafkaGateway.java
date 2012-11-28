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

package com.senseidb.gateway.test;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.javaapi.producer.ProducerData;
import kafka.message.Message;
import kafka.producer.ProducerConfig;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.json.JSONObject;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import proj.zoie.impl.indexing.StreamDataProvider;

import com.senseidb.gateway.SenseiGateway;
import com.senseidb.gateway.kafka.DefaultJsonDataSourceFilter;
import com.senseidb.indexing.DataSourceFilter;
import com.senseidb.plugin.SenseiPluginRegistry;

public class TestKafkaGateway {
  static File confFile = new File("src/test/resources/configs/kafka-gateway.properties");
  static File confFile2 = new File("src/test/resources/configs/simplekafka-gateway.properties");
  static File kafkaServerFile = new File("src/test/resources/configs/kafka-server.properties");

  static SenseiGateway kafkaGateway;

  static SenseiGateway simpleKafkaGateway;
  
  static SenseiPluginRegistry pluginRegistry;

  static Configuration config = null;
  
  static SenseiPluginRegistry pluginRegistry2;

  static Configuration config2 = null;
  
  static KafkaServer kafkaServer = null;
  
  static File kafkaLogFile = null;
  
  @BeforeClass
  public static void init() throws Exception{
    config = new PropertiesConfiguration(confFile);
    pluginRegistry = SenseiPluginRegistry.build(config);
    pluginRegistry.start();

    
    Properties kafkaProps = new Properties();
    kafkaProps.load(new FileReader(kafkaServerFile));
    
    kafkaLogFile = new File(kafkaProps.getProperty("log.dir"));
    FileUtils.deleteDirectory(kafkaLogFile);
    
    KafkaConfig kafkaConfig = new KafkaConfig(kafkaProps);
    kafkaServer = new KafkaServer(kafkaConfig);
    
    kafkaServer.startup();


    kafkaGateway = pluginRegistry.getBeanByFullPrefix("sensei.gateway", SenseiGateway.class);
    kafkaGateway.start();
    
    config2 = new PropertiesConfiguration(confFile2);
    pluginRegistry2 = SenseiPluginRegistry.build(config2);
    pluginRegistry2.start();

    simpleKafkaGateway = pluginRegistry2.getBeanByFullPrefix("sensei.gateway", SenseiGateway.class);
    simpleKafkaGateway.start();
    
    Properties props = new Properties();
    props.put("zk.connect", "localhost:2181");
    props.put("serializer.class", "kafka.serializer.DefaultEncoder");

    ProducerConfig producerConfig = new ProducerConfig(props);
    Producer<String,Message> kafkaProducer = new Producer<String,Message>(producerConfig);
    String topic = config2.getString("sensei.gateway.kafka.topic");
    List<ProducerData<String, Message>> msgList = new ArrayList<ProducerData<String, Message>>();
    for (JSONObject jsonObj : BaseGatewayTestUtil.dataList){
      Message m = new Message(jsonObj.toString().getBytes(DefaultJsonDataSourceFilter.UTF8));
      ProducerData<String,Message> msg = new ProducerData<String,Message>(topic,m);
      msgList.add(msg);
    }
    kafkaProducer.send(msgList);
  }
  
  @AfterClass
  public static void shutdown() {
    kafkaGateway.stop();
    pluginRegistry.stop();
    
    simpleKafkaGateway.stop();
    pluginRegistry2.stop();
    
    try{
      if (kafkaServer!=null){
        kafkaServer.shutdown();
        kafkaServer.awaitShutdown();
      }
    }
    finally{
      try {
        FileUtils.deleteDirectory(kafkaLogFile);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }
  
  @Test
  public void testSimpleKafka() throws Exception{
    final StreamDataProvider<JSONObject> dataProvider =  simpleKafkaGateway.buildDataProvider((DataSourceFilter)null, String.valueOf("0"), null, null);
    BaseGatewayTestUtil.doTest(dataProvider);
  }
  

  @Test
  public void testKafka() throws Exception{
 //   final StreamDataProvider<JSONObject> dataProvider = kafkaGateway.buildDataProvider(null, String.valueOf("0"), null, null);
   // BaseGatewayTestUtil.doTest(dataProvider);
  }
}
