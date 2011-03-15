package com.sensei.dataprovider.kafka;

import java.io.IOException;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;

public class KafkaAvroStreamDataProvider<D> extends KafkaStreamDataProvider<D> {

	private BinaryDecoder binDecoder;
	private final SpecificDatumReader<D> reader;
	private final Class<D> _cls;
	
	public KafkaAvroStreamDataProvider(String kafkaHost, int kafkaPort,
			int soTimeout, int batchSize, String topic, long startingOffset,Class<D> cls) {
		super(kafkaHost, kafkaPort, soTimeout, batchSize, topic, startingOffset);
		binDecoder = null;
		_cls = cls;
		reader = new SpecificDatumReader<D>(_cls);
	}

	@Override
	protected D convertMessageBytes(long msgStreamOffset, byte[] bytes,
			int offset, int size) throws IOException {
		try{
		  binDecoder = DecoderFactory.defaultFactory().createBinaryDecoder(bytes,offset,size, binDecoder);
		  D data = _cls.newInstance();
		  reader.read(data,binDecoder);
		  return data;
		}
		catch(Exception e){
		  throw new IOException(e.getMessage(),e);
		}
	}
}
