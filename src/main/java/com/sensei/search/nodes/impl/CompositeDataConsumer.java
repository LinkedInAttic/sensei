package com.sensei.search.nodes.impl;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import proj.zoie.api.DataConsumer;
import proj.zoie.api.ZoieException;
import proj.zoie.api.ZoieVersion;

public class CompositeDataConsumer<T,V extends ZoieVersion> implements DataConsumer<T,V> {

	private List<DataConsumer<T,V>> _consumerList;
	public CompositeDataConsumer(){
		_consumerList = new LinkedList<DataConsumer<T,V>>();
	}
	
	public void addDataConsumer(DataConsumer<T,V> dataConsumer){
		_consumerList.add(dataConsumer);
	}
	
	@Override
	public void consume(Collection<DataEvent<T,V>> events)
			throws ZoieException {
		for (DataConsumer<T,V> consumer : _consumerList){
			consumer.consume(events);
		}
	}

	@Override
	public V getVersion() {
		V version = null;
		if (_consumerList!=null){
		  for (DataConsumer<T,V> consumer : _consumerList){
			V ver = consumer.getVersion();
			if (ver.compareTo(version)<0){
				version = ver;
			}
		  }
		}
		return version;
	}

}
