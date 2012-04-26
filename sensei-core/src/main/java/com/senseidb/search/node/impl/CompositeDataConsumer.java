package com.senseidb.search.node.impl;

import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;

import com.linkedin.zoie.api.DataConsumer;
import com.linkedin.zoie.api.ZoieException;

public class CompositeDataConsumer<T> implements DataConsumer<T> {

  private List<DataConsumer<T>> _consumerList;
  private Comparator<String> _versionComparator;
  public CompositeDataConsumer(Comparator<String> versionComparator){
    _consumerList = new LinkedList<DataConsumer<T>>();
    _versionComparator = versionComparator;
  }
  
  public void addDataConsumer(DataConsumer<T> dataConsumer){
    _consumerList.add(dataConsumer);
  }
  
  @Override
  public void consume(Collection<DataEvent<T>> events)
      throws ZoieException {
    for (DataConsumer<T> consumer : _consumerList){
      consumer.consume(events);
    }
  }

  @Override
  public String getVersion() {
    String version = null;
    if (_consumerList!=null){
      for (DataConsumer<T> consumer : _consumerList){
        String ver = consumer.getVersion();
        if (_versionComparator.compare(ver, version)<0){
          version = ver;
        }
      }
    }
    return version;
  }

  @Override
  public Comparator<String> getVersionComparator() {
    return _versionComparator;
  }
}
