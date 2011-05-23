package com.sensei.indexing.api.gateway;

import java.util.HashMap;
import java.util.Map;

import com.sensei.indexing.api.gateway.file.LinedFileDataProviderBuilder;
import com.sensei.indexing.api.gateway.jdbc.JdbcDataProviderBuilder;
import com.sensei.indexing.api.gateway.jms.JmsDataProviderBuilder;
import com.sensei.indexing.api.gateway.kafka.KafkaDataProviderBuilder;

public class SenseiGatewayRegistry {
	
	static Map<String,SenseiGateway<?>> registry = new HashMap<String,SenseiGateway<?>>();
	
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
	
	public static SenseiGateway<?> getDataProviderBuilder(String name){
		return registry.get(name);
	}
}
