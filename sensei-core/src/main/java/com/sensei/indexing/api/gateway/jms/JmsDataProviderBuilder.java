package com.sensei.indexing.api.gateway.jms;

import java.util.Comparator;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TopicConnectionFactory;

import org.apache.commons.configuration.ConfigurationException;
import org.json.JSONObject;

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
  private final Comparator<String> _versionComparator = ZoieConfig.DEFAULT_VERSION_COMPARATOR;




	@Override
	public StreamDataProvider<JSONObject> buildDataProvider(final DataSourceFilter<Message> dataFilter,
			String oldSinceKey) throws Exception{

	    final String topic = config.get("topic");
	    final String clientID = config.get("clientId");
	    final String topicFac = config.get("topicFactory");

	    TopicFactory topicFactory = pluginRegistry.getBeanByFullPrefix(name + ".topicFactory", TopicFactory.class);

	    if (topicFactory == null){
	    	throw new ConfigurationException("topicFactory not defined: "+topicFac);
	    }

	    TopicConnectionFactory connectionFactory = pluginRegistry.getBeanByFullPrefix(name + ".connectionFactory", TopicConnectionFactory.class);

	    if (connectionFactory == null){
	    	throw new ConfigurationException("topic connection factory not defined.");
	    }

	    DataEventBuilder<JSONObject> dataEventBuilder = new DataEventBuilder<JSONObject>() {
	    	final DataSourceFilter<Message> filter = dataFilter;
			@Override
			public DataEvent<JSONObject> buildDataEvent(Message message)
					throws JMSException {

				try {
					return new DataEvent<JSONObject>(filter.filter(message), String.valueOf(System.nanoTime()));
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
