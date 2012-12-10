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
