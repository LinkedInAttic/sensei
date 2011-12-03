package com.sensei.indexing.api.gateway;

import java.util.HashMap;
import java.util.Map;

import com.sensei.indexing.api.gateway.file.LinedFileDataProviderBuilder;
import com.sensei.indexing.api.gateway.jdbc.JdbcDataProviderBuilder;
import com.sensei.indexing.api.gateway.jms.JmsDataProviderBuilder;
import com.sensei.indexing.api.gateway.kafka.KafkaDataProviderBuilder;

public class SenseiGatewayRegistry {
	
	static Map<String,Class> registry = new HashMap<String,Class>();
	
	static{
		registry.put(LinedFileDataProviderBuilder.name, LinedFileDataProviderBuilder.class);
		registry.put(KafkaDataProviderBuilder.name, KafkaDataProviderBuilder.class);
		registry.put(JmsDataProviderBuilder.name, JmsDataProviderBuilder.class);
		registry.put(JdbcDataProviderBuilder.name, JdbcDataProviderBuilder.class);
	}
	
	public static Class getGatewayClass(String name){
		return registry.get(name);
	}
}
