package com.sensei.search.nodes.impl;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import proj.zoie.api.DataConsumer;
import proj.zoie.api.ZoieException;

public class CompositeDataConsumer<V> implements DataConsumer<V> {

	private List<DataConsumer<V>> _consumerList;
	public CompositeDataConsumer(){
		_consumerList = new LinkedList<DataConsumer<V>>();
	}
	
	public void addDataConsumer(DataConsumer<V> dataConsumer){
		_consumerList.add(dataConsumer);
	}
	
	@Override
	public void consume(Collection<proj.zoie.api.DataConsumer.DataEvent<V>> events)
			throws ZoieException {
		for (DataConsumer<V> consumer : _consumerList){
			consumer.consume(events);
		}
	}

	@Override
	public long getVersion() {
		long version = Long.MAX_VALUE;
		if (_consumerList==null || _consumerList.size() == 0){
			return 0L;
		}
		
		for (DataConsumer<V> consumer : _consumerList){
			long ver = consumer.getVersion();
			if (ver < version){
				version = ver;
			}
		}
		
		return version;
	}

}
