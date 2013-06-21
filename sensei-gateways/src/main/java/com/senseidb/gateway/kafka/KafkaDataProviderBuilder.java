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

package com.senseidb.gateway.kafka;

import java.util.Comparator;
import java.util.Properties;
import java.util.Set;

import org.json.JSONObject;

import proj.zoie.impl.indexing.StreamDataProvider;
import proj.zoie.impl.indexing.ZoieConfig;

import com.senseidb.gateway.SenseiGateway;
import com.senseidb.indexing.DataSourceFilter;
import com.senseidb.indexing.ShardingStrategy;

public class KafkaDataProviderBuilder extends SenseiGateway<DataPacket>{

	private final Comparator<String> _versionComparator = ZoieConfig.DEFAULT_VERSION_COMPARATOR;

  private void extractProperty(Properties props, String key)
  {
    String value = config.get("kafka." + key);
    if (value != null && value.length() != 0)
    {
      props.setProperty(key, value);
    }
  }

	@Override
  public StreamDataProvider<JSONObject> buildDataProvider(DataSourceFilter<DataPacket> dataFilter,
      String oldSinceKey,
      ShardingStrategy shardingStrategy,
      Set<Integer> partitions) throws Exception
  {
	  String zookeeperUrl = config.get("kafka.zookeeperUrl");
	  String consumerGroupId = config.get("kafka.consumerGroupId");
    String topic = config.get("kafka.topic");
    String timeoutStr = config.get("kafka.timeout");
    int timeout = timeoutStr != null ? Integer.parseInt(timeoutStr) : 10000;
    int batchsize = Integer.parseInt(config.get("kafka.batchsize"));

    Properties props = new Properties();
    extractProperty(props, "socket.timeout.ms");
    extractProperty(props, "socket.buffersize");
    extractProperty(props, "fetch.size");
    extractProperty(props, "backoff.increment.ms");
    extractProperty(props, "queuedchunks.max");
    extractProperty(props, "autocommit.interval.ms");
    extractProperty(props, "rebalance.retries.max");

    long offset = oldSinceKey == null ? 0L : Long.parseLong(oldSinceKey);
    
    if (dataFilter==null){
      String type = config.get("kafka.msg.type");
      if (type == null){
        type = "json";
      }
    
      if ("json".equals(type)){
        dataFilter = new DefaultJsonDataSourceFilter();
      }
      else if ("avro".equals(type)){
        String msgClsString = config.get("kafka.msg.avro.class");
        String dataMapperClassString = config.get("kafka.msg.avro.datamapper");
        Class cls = Class.forName(msgClsString);
        Class dataMapperClass = Class.forName(dataMapperClassString);
        DataSourceFilter dataMapper = (DataSourceFilter)dataMapperClass.newInstance();
        dataFilter = new AvroDataSourceFilter(cls, dataMapper);
      }
      else{
        throw new IllegalArgumentException("invalid msg type: "+type);
      }
    }
    
		KafkaStreamDataProvider provider = new KafkaStreamDataProvider(_versionComparator,
                                                                   zookeeperUrl,
                                                                   timeout,
                                                                   batchsize,
                                                                   consumerGroupId,
                                                                   topic,
                                                                   offset,
                                                                   dataFilter,
                                                                   props);
		return provider;
	}

  @Override
  public Comparator<String> getVersionComparator() {
    return _versionComparator;
  }
}
