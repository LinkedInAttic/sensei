//package com.senseidb.gateway.test;
//
//import java.io.File;
//import java.io.FileReader;
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.List;
//import java.util.Properties;
//
//import kafka.javaapi.producer.Producer;
//import kafka.producer.KeyedMessage;
//import kafka.producer.ProducerConfig;
//import kafka.server.KafkaConfig;
//import kafka.server.KafkaServer;
//
//import org.apache.commons.configuration.Configuration;
//import org.apache.commons.configuration.PropertiesConfiguration;
//import org.apache.commons.io.FileUtils;
//import org.json.JSONObject;
//import org.junit.AfterClass;
//import org.junit.BeforeClass;
//import org.junit.Test;
//
//import proj.zoie.impl.indexing.StreamDataProvider;
//
//import com.senseidb.gateway.SenseiGateway;
//import com.senseidb.gateway.kafka.DefaultJsonDataSourceFilter;
//import com.senseidb.plugin.SenseiPluginRegistry;
//
//public class TestKafkaGateway {
//  static File confFile = new File("src/test/resources/configs/kafka-gateway.properties");
////  static File confFile2 = new File("src/test/resources/configs/simplekafka-gateway.properties");
//  static File kafkaServerFile = new File("src/test/resources/configs/kafka-server.properties");
//
//  static SenseiGateway kafkaGateway;
//
//  static Producer<String,byte[]> kafkaProducer = null;
//
////  static SenseiGateway simpleKafkaGateway;
//
//  static SenseiPluginRegistry pluginRegistry;
//
//  static Configuration config = null;
//
////  static SenseiPluginRegistry pluginRegistry2;
//
////  static Configuration config2 = null;
//
//  static KafkaServer kafkaServer = null;
//
//  static File kafkaLogFile = null;
//
//  static List<KeyedMessage<String, byte[]>> _messageList= null;
//
//  @BeforeClass
//  public static void init() throws Exception{
//    config = new PropertiesConfiguration(confFile);
//    pluginRegistry = SenseiPluginRegistry.build(config);
//    pluginRegistry.start();
//
//
//    Properties kafkaProps = new Properties();
//    kafkaProps.load(new FileReader(kafkaServerFile));
//
//    kafkaLogFile = new File(kafkaProps.getProperty("log.dir"));
//    FileUtils.deleteDirectory(kafkaLogFile);
//
//    KafkaConfig kafkaConfig = new KafkaConfig(kafkaProps);
//    kafkaServer = new KafkaServer(kafkaConfig, new kafka.utils.Time() {
//      @Override
//      public long milliseconds() {
//        return System.currentTimeMillis();
//      }
//
//      @Override
//      public long nanoseconds() {
//        return System.nanoTime();
//      }
//
//      @Override
//      public void sleep(long ms) {
//        try {
//          Thread.sleep(ms);
//        } catch (InterruptedException e) {
//          System.err.println("NOOOOOOOO");
//        }
//      }
//    });
//
//    kafkaServer.startup();
//
//
//    kafkaGateway = pluginRegistry.getBeanByFullPrefix("sensei.gateway", SenseiGateway.class);
//    kafkaGateway.start();
//
////    config2 = new PropertiesConfiguration(confFile2);
////    pluginRegistry2 = SenseiPluginRegistry.build(config2);
////    pluginRegistry2.start();
//
////    simpleKafkaGateway = pluginRegistry2.getBeanByFullPrefix("sensei.gateway", SenseiGateway.class);
////    simpleKafkaGateway.start();
//
//    Properties props = new Properties();
//    props.put("zookeeper.connect", "localhost:2181");
//    props.put("serializer.class", "kafka.serializer.DefaultEncoder");
//    props.put("broker.id", "1");
//    props.put("metadata.broker.list", "localhost:9092");
//
//    ProducerConfig producerConfig = new ProducerConfig(props);
//    kafkaProducer = new Producer<String,byte[]>(producerConfig);
//    String topic = config.getString("sensei.gateway.kafka.topic");
//
//
//
//    List<KeyedMessage<String, byte[]>> msgList = new ArrayList<KeyedMessage<String, byte[]>>();
//    for (JSONObject jsonObj : BaseGatewayTestUtil.dataList){
//      KeyedMessage<String,byte[]> msg = new KeyedMessage<String,byte[]>(topic,
//          jsonObj.toString().getBytes(DefaultJsonDataSourceFilter.UTF8));
//      msgList.add(msg);
//    }
//    _messageList = msgList;
//    kafkaProducer.send(msgList);
//  }
//
//  @AfterClass
//  public static void shutdown() {
//    kafkaGateway.stop();
//    pluginRegistry.stop();
//
////    simpleKafkaGateway.stop();
////    pluginRegistry2.stop();
//
//    try{
//      if (kafkaServer!=null){
//        kafkaServer.shutdown();
//        kafkaServer.awaitShutdown();
//      }
//    }
//    finally{
//      try {
//        FileUtils.deleteDirectory(kafkaLogFile);
//      } catch (IOException e) {
//        e.printStackTrace();
//      }
//    }
//  }
//
////  @Test
////  public void testSimpleKafka() throws Exception{
////    final StreamDataProvider<JSONObject> dataProvider =  simpleKafkaGateway.buildDataProvider(null, String.valueOf("0"), null, null);
////    BaseGatewayTestUtil.doTest(dataProvider);
////  }
////
//
//  @Test
//  public void testKafka() throws Exception{
//    final StreamDataProvider<JSONObject> dataProvider = kafkaGateway.buildDataProvider(null, String.valueOf("0"), null, null);
//    BaseGatewayTestUtil.doTest(dataProvider);
//  }
//}
