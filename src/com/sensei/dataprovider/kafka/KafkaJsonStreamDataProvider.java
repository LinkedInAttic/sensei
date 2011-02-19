package com.sensei.dataprovider.kafka;

import java.io.IOException;
import java.nio.charset.Charset;

import org.json.JSONException;
import org.json.JSONObject;

public class KafkaJsonStreamDataProvider extends KafkaStreamDataProvider<JSONObject> {
    private final static Charset UTF8 = Charset.forName("UTF-8");
    
	public KafkaJsonStreamDataProvider(String kafkaHost, int kafkaPort,
			int soTimeout, int batchSize, String topic, long startingOffset) {
		super(kafkaHost, kafkaPort, soTimeout, batchSize, topic, startingOffset);
	}

	@Override
	protected JSONObject convertMessageBytes(long msgStreamOffset, byte[] bytes,
			int offset, int size) throws IOException {
		String jsonString = new String(bytes,offset,size,UTF8);
		
		try {
			return new JSONObject(jsonString);
		} catch (JSONException e) {
			throw new IOException(e.getMessage(),e);
		}
	}

}
