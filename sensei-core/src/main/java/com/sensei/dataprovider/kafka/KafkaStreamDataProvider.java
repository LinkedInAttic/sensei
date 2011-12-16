package com.sensei.dataprovider.kafka;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaMessageStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.Message;

import org.apache.log4j.Logger;

import proj.zoie.api.DataConsumer.DataEvent;
import proj.zoie.impl.indexing.StreamDataProvider;

public abstract class KafkaStreamDataProvider<D> extends StreamDataProvider<D>{

  private final String _topic;
  private final String _consumerGroupId;
  private ConsumerConnector _consumerConnector;
  private ConsumerIterator<Message> _consumerIterator;

  private ThreadLocal<byte[]> bytesFactory;

  private static Logger logger = Logger.getLogger(KafkaStreamDataProvider.class);
  
    public static final int DEFAULT_MAX_MSG_SIZE = 5*1024*1024;
    private final String _zookeeperUrl;
    private final int _kafkaSoTimeout;
    private volatile boolean _started = false;
  
  public KafkaStreamDataProvider(Comparator<String> versionComparator,String zookeeperUrl,int soTimeout,int batchSize,
                                 String consumerGroupId,String topic,long startingOffset){
    super(versionComparator);
    _consumerGroupId = consumerGroupId;
    _topic = topic;
    super.setBatchSize(batchSize);
    _zookeeperUrl = zookeeperUrl;
    _kafkaSoTimeout = soTimeout;
    _consumerConnector = null;
    _consumerIterator = null;
    bytesFactory = new ThreadLocal<byte[]>(){
      @Override
      protected byte[] initialValue() {
        return new byte[DEFAULT_MAX_MSG_SIZE];
      }
    };
  }
  
  @Override
  public void setStartingOffset(String version){
  }
  
  protected D convertMessage(long msgStreamOffset,Message msg) throws IOException{
    int size = msg.payloadSize();
    ByteBuffer byteBuffer = msg.payload();
    byte[] bytes = bytesFactory.get();
    byteBuffer.get(bytes,0,size);
    return convertMessageBytes(msgStreamOffset,bytes,0,size);
  }
  
  protected abstract D convertMessageBytes(long msgStreamOffset,byte[] bytes,int offset,int size) throws IOException;
  
  @Override
  public DataEvent<D> next() {
    if (!_started) return null;

    try
    {
      if (!_consumerIterator.hasNext())
        return null;
    }
    catch (Exception e)
    {
      // Most likely timeout exception - ok to ignore
      return null;
    }

    Message msg = _consumerIterator.next();
    if (logger.isDebugEnabled()){
      logger.debug("got new message: "+msg);
    }
    long version = System.currentTimeMillis();

    D data;
    try {
      data = convertMessage(version,msg);
      if (logger.isDebugEnabled()){
        logger.debug("message converted: "+data);
      }
      return new DataEvent<D>(data, String.valueOf(version));
    } catch (IOException e) {
      logger.error(e.getMessage(),e);
      return null;
    }
  }

  @Override
  public void reset() {
  }

  @Override
  public void start() {
    Properties props = new Properties();
    props.put("zk.connect", _zookeeperUrl);
    //props.put("consumer.timeout.ms", _kafkaSoTimeout);
    props.put("groupid", _consumerGroupId);

    ConsumerConfig consumerConfig = new ConsumerConfig(props);
    _consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);

    Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
    topicCountMap.put(_topic, 1);
    Map<String, List<KafkaMessageStream<Message>>> topicMessageStreams =
        _consumerConnector.createMessageStreams(topicCountMap);
    List<KafkaMessageStream<Message>> streams = topicMessageStreams.get(_topic);
    KafkaMessageStream<Message> kafkaMessageStream = streams.iterator().next();
    _consumerIterator = kafkaMessageStream.iterator();

    super.start();
    _started = true;
  }

  @Override
  public void stop() {
    _started = false;
    try{
      super.stop();
    }
    finally{
      if (_consumerConnector!=null){
      _consumerConnector.shutdown();
      }
    }
  }  
}
