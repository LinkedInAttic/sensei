package com.sensei.dataprovider.kafka;

import java.io.IOException;
import java.nio.ByteBuffer;

import kafka.api.FetchRequest;
import kafka.api.OffsetRequest;
import kafka.consumer.SimpleConsumer;
import kafka.message.ByteBufferMessageSet;
import kafka.message.Message;
import kafka.message.MessageSet;

import org.apache.log4j.Logger;

import proj.zoie.api.DefaultZoieVersion;
import proj.zoie.api.DataConsumer.DataEvent;
import proj.zoie.impl.indexing.StreamDataProvider;
import scala.collection.Iterator;

public abstract class KafkaStreamDataProvider<D> extends StreamDataProvider<D, DefaultZoieVersion> {
	private final String _topic;
	private long _offset;
	private long _startingOffset;
	private SimpleConsumer _kafkaConsumer;
	
	private Iterator<Message> _msgIter;
	private ThreadLocal<byte[]> bytesFactory;
	
	private static Logger logger = Logger.getLogger(KafkaStreamDataProvider.class);
  
    public static final int DEFAULT_MAX_MSG_SIZE = 5*1024*1024;
    private final String _kafkaHost;
    private final int _kafkaPort;
    private final int _kafkaSoTimeout;
    private volatile boolean _started = false;
	
	public KafkaStreamDataProvider(String kafkaHost,int kafkaPort,int soTimeout,int batchSize,String topic,long startingOffset){
		_topic = topic;
		_startingOffset = startingOffset;
		_offset = startingOffset;
		super.setBatchSize(batchSize);
		_kafkaHost = kafkaHost;
		_kafkaPort = kafkaPort;
		_kafkaSoTimeout = soTimeout;
		_kafkaConsumer = null;
		_msgIter = null;
		bytesFactory = new ThreadLocal<byte[]>(){
			@Override
			protected byte[] initialValue() {
				return new byte[DEFAULT_MAX_MSG_SIZE];
			}
		};
	}
	
	@Override
	public void setStartingOffset(DefaultZoieVersion version){
	    _offset = version.getVersionId();
	}
	
	private FetchRequest buildReq(){
		if (_offset<=0){
			long time = OffsetRequest.EARLIEST_TIME();
			if (_offset==-1){
				time = -OffsetRequest.LATEST_TIME();
			}
			_offset = _kafkaConsumer.getOffsetsBefore(_topic, 0, time, 1)[0];
		}
		return new FetchRequest(_topic, 0, _offset,DEFAULT_MAX_MSG_SIZE );
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
	public DataEvent<D, DefaultZoieVersion> next() {
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
		
		Message msg = _msgIter.next();
		if (logger.isDebugEnabled()){
		  logger.debug("got new message: "+msg);
		}
		DefaultZoieVersion version = new DefaultZoieVersion();
		version.setVersionId(_offset);
		_offset += MessageSet.entrySize(msg);

		D data;
		try {
			data = convertMessage(version.getVersionId(),msg);
			if (logger.isDebugEnabled()){
			  logger.debug("message converted: "+data);
			}
			return new DataEvent<D,DefaultZoieVersion>(data,version);
		} catch (IOException e) {
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
		  super.stop();
	  }
	  finally{
		  if (_kafkaConsumer!=null){
			_kafkaConsumer.close();
		  }
	  }
	}
}

