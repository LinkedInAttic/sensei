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
import kafka.producer.KeyedMessage;
import kafka.message.Message;
import kafka.producer.ProducerConfig;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.SystemTime$;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.json.JSONObject;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import proj.zoie.impl.indexing.StreamDataProvider;

import com.senseidb.gateway.SenseiGateway;
import com.senseidb.gateway.kafka.DefaultJsonDataSourceFilter;
import com.senseidb.indexing.DataSourceFilter;
import com.senseidb.plugin.SenseiPluginRegistry;


public class TestKafkaGateway
{
  static SenseiGateway kafkaGateway;

  static SenseiPluginRegistry pluginRegistry;

  static Configuration config = null;

  static KafkaServer kafkaServer = null;

  static File kafkaLogFile = null;

  static TestZkServer zkServer = new TestZkServer();

  @BeforeClass
  public static void init()
      throws Exception
  {
    zkServer.start();

    File confFile = new File(TestKafkaGateway.class.getClassLoader().getResource("configs/kafka-gateway.properties").toURI());
    config = new PropertiesConfiguration(confFile);
    pluginRegistry = SenseiPluginRegistry.build(config);
    pluginRegistry.start();

    Properties kafkaProps = new Properties();
    File kafkaServerFile = new File(TestKafkaGateway.class.getClassLoader().getResource("configs/kafka-server.properties").toURI());
    kafkaProps.load(new FileReader(kafkaServerFile));

    kafkaLogFile = new File(kafkaProps.getProperty("log.dir"));
    FileUtils.deleteDirectory(kafkaLogFile);

    KafkaConfig kafkaConfig = new KafkaConfig(kafkaProps);
    kafkaServer = new KafkaServer(kafkaConfig, SystemTime$.MODULE$);

    kafkaServer.startup();

    kafkaGateway = pluginRegistry.getBeanByFullPrefix("sensei.gateway", SenseiGateway.class);
    kafkaGateway.start();

    Properties props = new Properties();
    props.put("metadata.broker.list", "localhost:" + kafkaProps.getProperty("port"));
    props.put("serializer.class", "kafka.serializer.StringEncoder");

    ProducerConfig producerConfig = new ProducerConfig(props);
    Producer<String, String> kafkaProducer = new Producer<String, String>(producerConfig);
    String topic = config.getString("sensei.gateway.kafka.topic");
    List<KeyedMessage<String, String>> msgList = new ArrayList<KeyedMessage<String, String>>();
    for (JSONObject jsonObj : BaseGatewayTestUtil.readDataFile())
    {
      KeyedMessage<String, String> msg = new KeyedMessage<String, String>(topic, jsonObj.toString());
      msgList.add(msg);
    }
    kafkaProducer.send(msgList);
  }

  @AfterClass
  public static void shutdown()
  {
    kafkaGateway.stop();
    pluginRegistry.stop();

    try
    {
      if (kafkaServer != null)
      {
        kafkaServer.shutdown();
        kafkaServer.awaitShutdown();
      }
    }
    finally
    {
      try
      {
        FileUtils.deleteDirectory(kafkaLogFile);
      }
      catch (IOException e)
      {
        e.printStackTrace();
      }
    }

    zkServer.stop();
  }

  @Ignore
  @Test
  public void testSimpleKafka()
      throws Exception
  {
    final StreamDataProvider<JSONObject> dataProvider = kafkaGateway.buildDataProvider((DataSourceFilter) null,
                                                                                       "0",
                                                                                       null,
                                                                                       null);
    BaseGatewayTestUtil.doTest(dataProvider);
  }
}
