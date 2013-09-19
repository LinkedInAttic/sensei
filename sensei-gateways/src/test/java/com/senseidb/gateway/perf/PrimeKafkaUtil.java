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
import kafka.message.Message;

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
    Producer<String,Message> kafkaProducer = new Producer<String,Message>(producerConfig);
    String topic = "perfTopic";
    List<KeyedMessage<String, Message>> msgList = new ArrayList<KeyedMessage<String, Message>>();
   
    
    BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(f),UTF8));
    
    int batchSize = 10000;
    int count = 0;
    while(true){
      String line = reader.readLine();
      if (line==null) break;
      count++;
      System.out.println(count+" msgs pushed.");
      Message m = new Message(line.getBytes(DefaultJsonDataSourceFilter.UTF8));
        KeyedMessage<String,Message> msg = new KeyedMessage<String,Message>(topic,m);
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
