package com.sensei.indexing.api.gateway.jms;

import java.util.Comparator;
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
import proj.zoie.impl.indexing.ZoieConfig;

import com.sensei.indexing.api.DataSourceFilter;
import com.sensei.indexing.api.gateway.SenseiGateway;

public class JmsDataProviderBuilder extends SenseiGateway<Message>{

	public static final String name = "jms";
  private final Comparator<String> _versionComparator;
	
	private final AtomicLong _version = new AtomicLong();
	
	public JmsDataProviderBuilder(Configuration conf){
	  super(conf);
	  _versionComparator = ZoieConfig.DEFAULT_VERSION_COMPARATOR;
	}
	
	@Override
	public String getName() {
		return name;
	}

	@Override
	public StreamDataProvider<JSONObject> buildDataProvider(final DataSourceFilter<Message> dataFilter,
			String oldSinceKey,ApplicationContext plugin) throws Exception{

	    Configuration myConf = _conf.subset(name);
		  _version.set(Long.parseLong(oldSinceKey));
	    final String topic = myConf.getString("topic");
	    final String clientID = myConf.getString("clientId",null);
	    final String topicFac = myConf.getString("topicFactory");
	    
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
	    
		JMSStreamDataProvider<JSONObject> provider = new JMSStreamDataProvider<JSONObject>(topic, clientID, connectionFactory, topicFactory, dataEventBuilder, _versionComparator);
		return provider;
	}

  @Override
  public Comparator<String> getVersionComparator() {
    return _versionComparator;
  }
	
	
	
}
