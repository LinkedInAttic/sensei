package com.sensei.indexing.api.gateway.kafka;

import java.util.Comparator;

import org.json.JSONObject;

import proj.zoie.impl.indexing.StreamDataProvider;
import proj.zoie.impl.indexing.ZoieConfig;

import com.sensei.dataprovider.kafka.KafkaJsonStreamDataProvider;
import com.sensei.indexing.api.DataSourceFilter;
import com.sensei.indexing.api.gateway.SenseiGateway;

public class KafkaDataProviderBuilder extends SenseiGateway<byte[]>{

	private final Comparator<String> _versionComparator = ZoieConfig.DEFAULT_VERSION_COMPARATOR;



	@Override
	public StreamDataProvider<JSONObject> buildDataProvider(DataSourceFilter<byte[]> dataFilter,
			String oldSinceKey) throws Exception{

		String host = config.get("host");
		String portStr = config.get("port");
		int port = Integer.parseInt(portStr);
		String topic = config.get("topic");
		String timeoutStr = config.get("timeout");
		int timeout = timeoutStr != null ? Integer.parseInt(timeoutStr) : 10000;
		int batchsize = Integer.parseInt(config.get("batchsize"));
		long offset = oldSinceKey == null ? 0L : Long.parseLong(oldSinceKey);
		KafkaJsonStreamDataProvider provider = new KafkaJsonStreamDataProvider(_versionComparator, host,port,timeout,batchsize,topic,offset);
		if (dataFilter!=null){
		  provider.setFilter(dataFilter);
		}
		return provider;
	}

  @Override
  public Comparator<String> getVersionComparator() {
    return _versionComparator;
  }


}
