package com.sensei.search.nodes.impl;

import java.util.Collection;
import java.util.List;

import proj.zoie.api.DataConsumer;
import proj.zoie.api.ZoieException;
import proj.zoie.api.indexing.IndexingEventListener;
import proj.zoie.impl.indexing.AsyncDataConsumer;
import proj.zoie.impl.indexing.ZoieSystem;

public class CompositeAsyncDataConsumer<D> extends AsyncDataConsumer<D> implements IndexingEventListener{

  private final List<AsyncDataConsumer<D>> _consumerList;
  public CompositeAsyncDataConsumer(ZoieSystem<?,D> zoie,List<AsyncDataConsumer<D>> consumerList) {
    super(zoie.getVersionComparator());
    zoie.addIndexingEventListener(this);
    _consumerList = consumerList;
  }

  @Override
  public void handleIndexingEvent(IndexingEvent evt) {
    for (AsyncDataConsumer<D> consumer : _consumerList){
      
    }
  }

  @Override
  public void handleUpdatedDiskVersion(String version) {
    // TODO Auto-generated method stub
    
  }
  
  private static class CompositeDataConsumer<D> implements DataConsumer<D>{

    @Override
    public void consume(Collection<proj.zoie.api.DataConsumer.DataEvent<D>> data)
        throws ZoieException {
      // TODO Auto-generated method stub
      
    }

    @Override
    public String getVersion() {
      // TODO Auto-generated method stub
      return null;
    }
    
  }

}
