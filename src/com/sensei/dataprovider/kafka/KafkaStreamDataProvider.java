package com.sensei.dataprovider.kafka;

import java.io.IOException;
import java.nio.ByteBuffer;

import kafka.api.FetchRequest;
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
	private final SimpleConsumer _kafkaConsumer;
	
	private Iterator<Message> _msgIter;
	private ThreadLocal<byte[]> bytesFactory;
	
	private static Logger logger = Logger.getLogger(KafkaStreamDataProvider.class);
  
    public static final int DEFAULT_MAX_MSG_SIZE = 5*1024*1024;  	
	
	public KafkaStreamDataProvider(String kafkaHost,int kafkaPort,int soTimeout,int batchSize,String topic,long startingOffset){
		_topic = topic;
		_startingOffset = startingOffset;
		_offset = startingOffset;
		super.setBatchSize(batchSize);
		_kafkaConsumer = new SimpleConsumer(kafkaHost, kafkaPort, soTimeout, DEFAULT_MAX_MSG_SIZE);
		_msgIter = null;
		bytesFactory = new ThreadLocal<byte[]>(){
			@Override
			protected byte[] initialValue() {
				return new byte[DEFAULT_MAX_MSG_SIZE];
			}
		};
	}
	
	public void setStartingOffset(long offset){
	    _offset = offset;
	}
	
	private FetchRequest buildReq(){
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
		if(_msgIter==null || !_msgIter.hasNext()){
			logger.debug("fetching new batch from offset: "+_offset);
			FetchRequest req = buildReq();
			ByteBufferMessageSet msgSet = _kafkaConsumer.fetch(req);
			_msgIter = msgSet.iterator();
		}
		
		if (_msgIter==null || !_msgIter.hasNext() ) {
			logger.debug("no more data, msgIter: "+_msgIter);
			return null;
		}
		
		Message msg = _msgIter.next();
		logger.debug("got new message: "+msg);
		DefaultZoieVersion version = new DefaultZoieVersion();
		version.setVersionId(_offset);
		_offset += MessageSet.entrySize(msg);

		D data;
		try {
			
			data = convertMessage(version.getVersionId(),msg);
			logger.debug("message converted: "+data);
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
}

