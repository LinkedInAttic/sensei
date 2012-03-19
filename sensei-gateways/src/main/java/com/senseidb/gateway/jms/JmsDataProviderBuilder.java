package com.senseidb.gateway.jms;

import java.util.Comparator;
import java.util.Set;

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

import com.senseidb.gateway.SenseiGateway;
import com.senseidb.indexing.DataSourceFilter;
import com.senseidb.indexing.ShardingStrategy;

public class JmsDataProviderBuilder extends SenseiGateway<Message>{

	public static final String name = "jms";
  private final Comparator<String> _versionComparator = ZoieConfig.DEFAULT_VERSION_COMPARATOR;




	@Override
	public StreamDataProvider<JSONObject> buildDataProvider(final DataSourceFilter<Message> dataFilter,
      String oldSinceKey,
      ShardingStrategy shardingStrategy,
      Set<Integer> partitions) throws Exception
  {

	    final String topic = config.get("jms.topic");
	    final String clientID = config.get("jms.clientId");
	    final String topicFac = config.get("jms.topicFactory");

	    TopicFactory topicFactory = pluginRegistry.getBeanByName(topicFac, TopicFactory.class);
      if (topicFactory == null)
        topicFactory = pluginRegistry.getBeanByFullPrefix(topicFac, TopicFactory.class);

	    if (topicFactory == null){
	    	throw new ConfigurationException("topicFactory not defined: "+topicFac);
	    }

      TopicConnectionFactory connectionFactory = pluginRegistry.getBeanByName(config.get("jms.connectionFactory"), TopicConnectionFactory.class);

      if (connectionFactory == null)
        connectionFactory = pluginRegistry.getBeanByFullPrefix(config.get("jms.connectionFactory"), TopicConnectionFactory.class);

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
