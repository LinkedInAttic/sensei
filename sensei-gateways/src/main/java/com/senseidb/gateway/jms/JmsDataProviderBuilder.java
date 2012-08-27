/**
 * This software is licensed to you under the Apache License, Version 2.0 (the
 * "Apache License").
 *
 * LinkedIn's contributions are made under the Apache License. If you contribute
 * to the Software, the contributions will be deemed to have been made under the
 * Apache License, unless you expressly indicate otherwise. Please do not make any
 * contributions that would be inconsistent with the Apache License.
 *
 * You may obtain a copy of the Apache License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, this software
 * distributed under the Apache License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the Apache
 * License for the specific language governing permissions and limitations for the
 * software governed under the Apache License.
 *
 * © 2012 LinkedIn Corp. All Rights Reserved.  
 */

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
