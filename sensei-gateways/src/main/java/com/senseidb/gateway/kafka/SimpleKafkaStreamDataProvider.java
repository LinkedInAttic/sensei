package com.senseidb.gateway.kafka;

import java.nio.ByteBuffer;
import java.util.Comparator;

import kafka.api.FetchRequest;
import kafka.api.OffsetRequest;
import kafka.consumer.SimpleConsumer;
import kafka.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;

import org.apache.log4j.Logger;
import org.json.JSONObject;

import com.linkedin.zoie.api.DataConsumer.DataEvent;
import com.linkedin.zoie.impl.indexing.StreamDataProvider;
import scala.collection.Iterator;

import com.senseidb.indexing.DataSourceFilter;

public class SimpleKafkaStreamDataProvider extends StreamDataProvider<JSONObject> {
  private final String _topic;
  private long _offset;
  private long _startingOffset;
  private SimpleConsumer _kafkaConsumer;
  
  private Iterator<MessageAndOffset> _msgIter;
  private ThreadLocal<byte[]> bytesFactory;
  
  private static Logger logger = Logger.getLogger(KafkaStreamDataProvider.class);
  
    public static final int DEFAULT_MAX_MSG_SIZE = 5*1024*1024;
    private final String _kafkaHost;
    private final int _kafkaPort;
    private final int _kafkaSoTimeout;
    private volatile boolean _started = false;
    private final DataSourceFilter<DataPacket> _dataConverter;
  
  public SimpleKafkaStreamDataProvider(Comparator<String> versionComparator, String kafkaHost,int kafkaPort,int soTimeout,int batchSize,String topic,long startingOffset,DataSourceFilter<DataPacket> dataConverter){
    super(versionComparator);
    _topic = topic;
    _startingOffset = startingOffset;
    _offset = startingOffset;
    super.setBatchSize(batchSize);
    _kafkaHost = kafkaHost;
    _kafkaPort = kafkaPort;
    _kafkaSoTimeout = soTimeout;
    _kafkaConsumer = null;
    _msgIter = null;
    _dataConverter = dataConverter;
    if (_dataConverter == null){
      throw new IllegalArgumentException("kafka data converter is null");
    }
     bytesFactory = new ThreadLocal<byte[]>(){
      @Override
      protected byte[] initialValue() {
        return new byte[DEFAULT_MAX_MSG_SIZE];
      }
    };
  }
  
  @Override
  public void setStartingOffset(String version){
      _offset = Long.parseLong(version);
  }
  
  private FetchRequest buildReq(){
    if (_offset<=0){
      long time = OffsetRequest.EarliestTime();
      if (_offset==-1){
        time = -OffsetRequest.LatestTime();
      }
      _offset = _kafkaConsumer.getOffsetsBefore(_topic, 0, time, 1)[0];
    }
    return new FetchRequest(_topic, 0, _offset,DEFAULT_MAX_MSG_SIZE );
  }
  
  @Override
  public DataEvent<JSONObject> next() {
    if (!_started) return null;
    if(_msgIter==null || !_msgIter.hasNext()){
      if (logger.isDebugEnabled()){
        logger.debug("fetching new batch from offset: "+_offset);
      }
      FetchRequest req = buildReq();
      ByteBufferMessageSet msgSet = _kafkaConsumer.fetch(req);
      _msgIter = msgSet.iterator();
    }
    
    if (_msgIter==null || !_msgIter.hasNext() ) {
      if (logger.isDebugEnabled()){
        logger.debug("no more data, msgIter: "+_msgIter);
      }
      return null;
    }
    
    MessageAndOffset msg = _msgIter.next();
    if (logger.isDebugEnabled()){
      logger.debug("got new message: "+msg);
    }
    long version = _offset;
    _offset = msg.offset();
    
    JSONObject data;
    try {
      int size = msg.message().payloadSize();
      ByteBuffer byteBuffer = msg.message().payload();
      byte[] bytes = bytesFactory.get();
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
    _offset = _startingOffset;
  }

  @Override
  public void start() {
    _kafkaConsumer = new SimpleConsumer(_kafkaHost, _kafkaPort, _kafkaSoTimeout, DEFAULT_MAX_MSG_SIZE);
    super.start();
    _started = true;
  }

  @Override
  public void stop() {
    _started = false;
    try{
      if (_kafkaConsumer!=null){
        _kafkaConsumer.close();
      }
    }
    finally{
      super.stop(); 
    }
  }
}
