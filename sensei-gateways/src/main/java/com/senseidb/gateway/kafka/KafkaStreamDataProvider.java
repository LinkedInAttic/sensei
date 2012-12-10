package com.senseidb.gateway.kafka;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaMessageStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.Message;

import org.apache.log4j.Logger;
import org.json.JSONObject;

import proj.zoie.api.DataConsumer.DataEvent;
import proj.zoie.impl.indexing.StreamDataProvider;

import com.senseidb.indexing.DataSourceFilter;

public class KafkaStreamDataProvider extends StreamDataProvider<JSONObject>{

  private final Set<String> _topics;
  private final String _consumerGroupId;
  private Properties _kafkaConfig;
  private ConsumerConnector _consumerConnector;
  private Iterator<Message> _consumerIterator;
  private ExecutorService _executorService;

  
  private static Logger logger = Logger.getLogger(KafkaStreamDataProvider.class);
    private final String _zookeeperUrl;
    private final int _kafkaSoTimeout;
    private volatile boolean _started = false;
    private final DataSourceFilter<DataPacket> _dataConverter;
  
  public KafkaStreamDataProvider(Comparator<String> versionComparator,String zookeeperUrl,int soTimeout,int batchSize,
                                 String consumerGroupId,String topic,long startingOffset,DataSourceFilter<DataPacket> dataConverter){
    this(versionComparator, zookeeperUrl, soTimeout, batchSize, consumerGroupId, topic, startingOffset, dataConverter, new Properties());
  }

  public KafkaStreamDataProvider(Comparator<String> versionComparator,String zookeeperUrl,int soTimeout,int batchSize,
                                 String consumerGroupId,String topic,long startingOffset,DataSourceFilter<DataPacket> dataConverter,Properties kafkaConfig){
    super(versionComparator);
    _consumerGroupId = consumerGroupId;
    _topics = new HashSet<String>();
    for (String raw : topic.split("[, ;]+"))
    {
      String t = raw.trim();
      if (t.length() != 0)
      {
        _topics.add(t);
      }
    }
    super.setBatchSize(batchSize);
    _zookeeperUrl = zookeeperUrl;
    _kafkaSoTimeout = soTimeout;
    _consumerConnector = null;
    _consumerIterator = null;

    _kafkaConfig = kafkaConfig;
    if (kafkaConfig == null) {
      kafkaConfig = new Properties();
    }

    _dataConverter = dataConverter;
    if (_dataConverter == null){
      throw new IllegalArgumentException("kafka data converter is null");
    }
  }
  
  @Override
  public void setStartingOffset(String version){
  }
  
  @Override
  public DataEvent<JSONObject> next() {
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
    
    JSONObject data;
    try {
      int size = msg.payloadSize();
      ByteBuffer byteBuffer = msg.payload();
      byte[] bytes = new byte[size];
      byteBuffer.get(bytes,0,size);
      data = _dataConverter.filter(new DataPacket(bytes,0,size));
      
      if (logger.isDebugEnabled()){
        logger.debug("message converted: "+data);
      }
      return new DataEvent<JSONObject>(data, String.valueOf(version));
    } catch (Exception e) {
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

    for (String key : _kafkaConfig.stringPropertyNames()) {
      props.put(key, _kafkaConfig.getProperty(key));
    }

    logger.info("Kafka properties: " + props);

    ConsumerConfig consumerConfig = new ConsumerConfig(props);
    _consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);

    Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
    for (String topic : _topics)
    {
      topicCountMap.put(topic, 1);
    }
    Map<String, List<KafkaMessageStream<Message>>> topicMessageStreams =
        _consumerConnector.createMessageStreams(topicCountMap);

    final ArrayBlockingQueue<Message> queue = new ArrayBlockingQueue<Message>(8, true);

    int streamCount = 0;
    for (List<KafkaMessageStream<Message>> streams : topicMessageStreams.values())
    {
      for (KafkaMessageStream<Message> stream : streams)
      {
        ++streamCount;
      }
    }
    _executorService = Executors.newFixedThreadPool(streamCount);

    for (List<KafkaMessageStream<Message>> streams : topicMessageStreams.values())
    {
      for (KafkaMessageStream<Message> stream : streams)
      {
        final KafkaMessageStream<Message> messageStream = stream;
        _executorService.execute(new Runnable()
          {
            @Override
            public void run()
            {
              logger.info("Kafka consumer thread started: " + Thread.currentThread().getId());
              try
              {
                for (Message message : messageStream)
                {
                  queue.put(message);
                }
              }
              catch(Exception e)
              {
                // normally it should the stop interupt exception.
                logger.error(e.getMessage(), e);
              }
              logger.info("Kafka consumer thread ended: " + Thread.currentThread().getId());
            }
          }
        );
      }
    }

    _consumerIterator = new Iterator<Message>()
    {
      private Message message = null;

      @Override
      public boolean hasNext()
      {
        if (message != null)  return true;

        try
        {
          message = queue.poll(1, TimeUnit.SECONDS);
        }
        catch(InterruptedException ie)
        {
          return false;
        }

        if (message != null)
        {
          return true;
        }
        else
        {
          return false;
        }
      }

      @Override
      public Message next()
      {
        if (hasNext())
        {
          Message res = message;
          message = null;
          return res;
        }
        else
        {
          throw new NoSuchElementException();
        }
      }

      @Override
      public void remove()
      {
        throw new UnsupportedOperationException("not supported");
      }
    };

    super.start();
    _started = true;
  }

  @Override
  public void stop() {
    _started = false;

    try
    {
      if (_executorService != null)
      {
        _executorService.shutdown();
      }
    }
    finally
    {
      try
      {
        if (_consumerConnector != null)
        {
          _consumerConnector.shutdown();
        }
      }
      finally
      {
        super.stop();
      }
    }
  }  
}
