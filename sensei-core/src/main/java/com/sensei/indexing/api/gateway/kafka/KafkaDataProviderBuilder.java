package com.sensei.indexing.api.gateway.kafka;

import java.util.Comparator;

import org.apache.commons.configuration.Configuration;
import org.json.JSONObject;
import org.springframework.context.ApplicationContext;

import proj.zoie.impl.indexing.StreamDataProvider;

import com.sensei.dataprovider.kafka.KafkaJsonStreamDataProvider;
import com.sensei.indexing.api.DataSourceFilter;
import com.sensei.indexing.api.gateway.SenseiGateway;

public class KafkaDataProviderBuilder extends SenseiGateway<byte[]>{

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
