package com.sensei.test;

import proj.zoie.api.DefaultZoieVersion;
import proj.zoie.api.Zoie;
import proj.zoie.api.DataConsumer.DataEvent;
import proj.zoie.impl.indexing.MemoryStreamDataProvider;

import com.browseengine.bobo.api.BoboIndexReader;
import com.sensei.search.nodes.SenseiIndexLoader;
import com.sensei.search.nodes.SenseiIndexLoaderFactory;
import com.sensei.search.svc.api.SenseiException;

public class SenseiExamples {
	
	public static class DummyIncrementSenseiIndexLoader implements SenseiIndexLoader{

		// underlying data stream
		private MemoryStreamDataProvider<Integer, DefaultZoieVersion> _dataStream;
		
		public DummyIncrementSenseiIndexLoader(Zoie<BoboIndexReader, Integer, DefaultZoieVersion> dataConsumer){
			
			// instantiating data stream
			_dataStream = new MemoryStreamDataProvider<Integer, DefaultZoieVersion>();
			
			// connect data stream to the data consumer/zoie instance
			_dataStream.setDataConsumer(dataConsumer);
			
			// creating data set
			Integer[] dataArray = new Integer[100];
			for (int i=0;i<dataArray.length;++i){
				dataArray[i]=i;
			}
			
			
			// build data events to send to stream, which will be piped into the dataConsumer/zoie instance
			DefaultZoieVersion dummyVersion = new DefaultZoieVersion();
			dummyVersion.setVersionId(0L);
			for (Integer data : dataArray){
			  DataEvent<Integer,DefaultZoieVersion> dataEvent = new DataEvent<Integer,DefaultZoieVersion>(data,dummyVersion);
			  _dataStream.addEvent(dataEvent);
			}
		}
		
		@Override
		public void shutdown() throws SenseiException {
			// flushing the stream to make sure data consumer/zoie handles left-over data
			_dataStream.flush();
			// shutting down the data stream
			_dataStream.stop();
		}

		@Override
		public void start() throws SenseiException {
			// data stream starts to stream and data will be pumped to data consumer/zoie
			_dataStream.start();
		}
		
	}
	
	public static class DummyIncrementSenseiIndexLoaderFactory implements SenseiIndexLoaderFactory<Integer, DefaultZoieVersion>{

		@Override
		public SenseiIndexLoader getIndexLoader(int partitionId,
				Zoie<BoboIndexReader, Integer, DefaultZoieVersion> dataConsumer) {
			return new DummyIncrementSenseiIndexLoader(dataConsumer);
		}
		
	}
}
