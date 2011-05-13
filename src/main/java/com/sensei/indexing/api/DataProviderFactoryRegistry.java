package com.sensei.indexing.api;

import java.io.File;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TopicConnectionFactory;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.json.JSONObject;
import org.springframework.context.ApplicationContext;

import proj.zoie.api.DataConsumer.DataEvent;
import proj.zoie.dataprovider.jms.DataEventBuilder;
import proj.zoie.dataprovider.jms.JMSStreamDataProvider;
import proj.zoie.dataprovider.jms.TopicFactory;
import proj.zoie.impl.indexing.StreamDataProvider;

import com.sensei.dataprovider.file.LinedJsonFileDataProvider;
import com.sensei.dataprovider.kafka.KafkaJsonStreamDataProvider;
import com.sensei.indexing.api.jdbc.JdbcDataProviderBuilder;

public class DataProviderFactoryRegistry {
	
	static Map<String,DataProviderBuilder<?>> registry = new HashMap<String,DataProviderBuilder<?>>();
	
	static{
		LinedFileDataProviderBuilder lineProvider = new LinedFileDataProviderBuilder();
		registry.put(lineProvider.getName(), lineProvider);
		
		KafkaDataProviderBuilder kafkaProvider = new KafkaDataProviderBuilder();
		registry.put(kafkaProvider.getName(), kafkaProvider);
		
		JmsDataProviderBuilder jmsProvider = new JmsDataProviderBuilder();
		registry.put(jmsProvider.getName(), jmsProvider);
		
		JdbcDataProviderBuilder jdbcProvider = new JdbcDataProviderBuilder();
		registry.put(jdbcProvider.getName(), jdbcProvider);
	}
	
	public static DataProviderBuilder<?> getDataProviderBuilder(String name){
		return registry.get(name);
	}

	public static abstract class DataProviderBuilder<V>{
		abstract public String getName();
		
		final public DataSourceFilter<V> getDataSourceFilter(Configuration conf,ApplicationContext pluginCtx){
			String filter = conf.getString("filter",null);
			if (filter!=null){
				return (DataSourceFilter<V>)pluginCtx.getBean(filter);
			}
			return null;
		}
		
		final public StreamDataProvider<JSONObject> buildDataProvider(Configuration conf,Comparator<String> versionComparator,
				String oldSinceKey,ApplicationContext plugin) throws Exception{
			DataSourceFilter<V> filter = getDataSourceFilter(conf,plugin);
			return buildDataProvider(conf,filter,versionComparator,oldSinceKey,plugin);
		}
		
		abstract public StreamDataProvider<JSONObject> buildDataProvider(Configuration conf,DataSourceFilter<V> dataFilter,Comparator<String> versionComparator,
				String oldSinceKey,ApplicationContext plugin) throws Exception;
	}
	
	public static class LinedFileDataProviderBuilder extends DataProviderBuilder<String>{

		public static final String name = "file";
		
		@Override
		public StreamDataProvider<JSONObject> buildDataProvider(
				Configuration conf,DataSourceFilter<String> dataFilter,Comparator<String> versionComparator,
				String oldSinceKey,ApplicationContext plugin) throws Exception{
			String path = conf.getString("path");
			long offset = oldSinceKey == null ? 0L : Long.parseLong(oldSinceKey);
			
			
			LinedJsonFileDataProvider provider = new LinedJsonFileDataProvider(versionComparator, new File(path), offset);
			if (dataFilter!=null){
			  provider.setFilter(dataFilter);
			}
			return provider;
		}
		
		@Override
		public String getName() {
			return name;
		}
	}
	
	public static class KafkaDataProviderBuilder extends DataProviderBuilder<byte[]>{

		public static final String name = "kafka";
		
		@Override
		public String getName() {
			return name;
		}

		@Override
		public StreamDataProvider<JSONObject> buildDataProvider(
				Configuration conf, DataSourceFilter<byte[]> dataFilter,Comparator<String> versionComparator,
				String oldSinceKey,ApplicationContext plugin) throws Exception{
			String host = conf.getString("host");
			int port = conf.getInt("port");
			String topic = conf.getString("topic");
			int timeout = conf.getInt("timeout",10000);
			int batchsize = conf.getInt("batchsize");
			long offset = oldSinceKey == null ? 0L : Long.parseLong(oldSinceKey);
			KafkaJsonStreamDataProvider provider = new KafkaJsonStreamDataProvider(versionComparator, host,port,timeout,batchsize,topic,offset);
			if (dataFilter!=null){
			  provider.setFilter(dataFilter);
			}
			return provider;
		}
	}
	
	
	
	public static class JmsDataProviderBuilder extends DataProviderBuilder<Message>{

		public static final String name = "jms";
		
		private final AtomicLong _version = new AtomicLong();
		@Override
		public String getName() {
			return name;
		}

		@Override
		public StreamDataProvider<JSONObject> buildDataProvider(
				Configuration conf, final DataSourceFilter<Message> dataFilter,Comparator<String> versionComparator,
				String oldSinceKey,ApplicationContext plugin) throws Exception{

			_version.set(Long.parseLong(oldSinceKey));
		    final String topic = conf.getString("topic");
		    final String clientID = conf.getString("clientId",null);
		    final String topicFac = conf.getString("topicFactory");
		    
		    TopicFactory topicFactory = (TopicFactory)plugin.getBean(topicFac);
		    
		    if (topicFactory == null){
		    	throw new ConfigurationException("topicFactory not defined: "+topicFac);
		    }
		    
		    TopicConnectionFactory connectionFactory = (TopicConnectionFactory)plugin.getBean("connectionFactory");
		    
		    if (connectionFactory == null){
		    	throw new ConfigurationException("topic connection factory not defined.");
		    }
		    
		    DataEventBuilder<JSONObject> dataEventBuilder = new DataEventBuilder<JSONObject>() {
		    	final DataSourceFilter<Message> filter = dataFilter;
				@Override
				public DataEvent<JSONObject> buildDataEvent(Message message)
						throws JMSException {
					
					try {
						return new DataEvent<JSONObject>(filter.filter(message), String.valueOf(_version.incrementAndGet()));
					} catch (Exception e) {
						throw new JMSException(e.getMessage());
					}
				}
		    	
			};
		    
			JMSStreamDataProvider<JSONObject> provider = new JMSStreamDataProvider<JSONObject>(topic, clientID, connectionFactory, topicFactory, dataEventBuilder, versionComparator);
			return provider;
		}
		
	}
}
