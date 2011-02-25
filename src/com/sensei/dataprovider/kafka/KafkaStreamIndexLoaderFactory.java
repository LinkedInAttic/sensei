package com.sensei.dataprovider.kafka;

import org.json.JSONObject;

import proj.zoie.api.DefaultZoieVersion;
import proj.zoie.impl.indexing.StreamDataProvider;

import com.sensei.search.nodes.impl.StreamIndexLoaderFactory;

public abstract class KafkaStreamIndexLoaderFactory<T> extends StreamIndexLoaderFactory<T,DefaultZoieVersion>{
	
	protected final String _host;
	protected final int _port;
	protected final String _topic;
	protected final int _soTimeout;
	protected final int _batchSize;
	
	public KafkaStreamIndexLoaderFactory(String host,int port,String topic,int batchSize,int soTimeout){
	  _host = host;
	  _port = port;
	  _topic = topic;
	  _batchSize = batchSize;
	  _soTimeout = soTimeout;
	}
	
	protected abstract KafkaStreamDataProvider<T> buildKafkaStreamProvider(int partitionId,long offset);

	@Override
	public StreamDataProvider<T,DefaultZoieVersion> buildStreamDataProvider(int partitionId,
			DefaultZoieVersion version) {
		
		// get the current offset to stream from
		long currentOffset = (version == null) ? 0 : version.getVersionId();
		return buildKafkaStreamProvider(partitionId,currentOffset);
	}
	
	public static class DefaultJsonFactory extends KafkaStreamIndexLoaderFactory<JSONObject>{

		public DefaultJsonFactory(String host, int port, String topic,
				int batchSize, int soTimeout) {
			super(host, port, topic, batchSize, soTimeout);
		}

		@Override
		protected KafkaStreamDataProvider<JSONObject> buildKafkaStreamProvider(
				int partitionId, long offset) {
			return new KafkaJsonStreamDataProvider(_host,_port,_soTimeout,_batchSize,_topic,offset);
		}
		
	}
}
