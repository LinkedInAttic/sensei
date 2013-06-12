package com.senseidb.gateway.perf;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import com.senseidb.gateway.kafka.DefaultJsonDataSourceFilter;

public class PrimeKafkaUtil {

  static Charset UTF8 = Charset.forName("UTF-8");
  /**
   * @param args
   */
  public static void main(String[] args) throws Exception{
    File f = new File("/home/jwang/github/search-perf/data/cars1m.json");
    
    Properties props = new Properties();
    props.put("zookeeper.connect", "localhost:2181");
    props.put("serializer.class", "kafka.serializer.DefaultEncoder");

    ProducerConfig producerConfig = new ProducerConfig(props);
    Producer<String, byte[]> kafkaProducer = new Producer<String,byte[]>(producerConfig);
    String topic = "perfTopic";
    List<KeyedMessage<String, byte[]>> msgList = new ArrayList<KeyedMessage<String, byte[]>>();
   
    
    BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(f),UTF8));
    
    int batchSize = 10000;
    int count = 0;
    while(true){
      String line = reader.readLine();
      if (line==null) break;
      count++;
      System.out.println(count+" msgs pushed.");
      KeyedMessage<String,byte[]> msg = new KeyedMessage<String,byte[]>(topic,line.getBytes(DefaultJsonDataSourceFilter.UTF8));
      msgList.add(msg);

      if (msgList.size()>batchSize){
        kafkaProducer.send(msgList);
        msgList.clear();
      }
    }

    if (msgList.size()>0){
      kafkaProducer.send(msgList);
    }
  }

}
