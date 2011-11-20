package com.sensei.search.nodes.impl;

import java.util.List;

import org.apache.log4j.Logger;

import proj.zoie.api.DataConsumer;
import proj.zoie.api.ZoieException;
import proj.zoie.api.indexing.IndexingEventListener;
import proj.zoie.impl.indexing.AsyncDataConsumer;
import proj.zoie.impl.indexing.ZoieSystem;

public class CompositeAsyncDataConsumer<D> extends AsyncDataConsumer<D> implements IndexingEventListener{

  private static final Logger logger = Logger.getLogger(CompositeAsyncDataConsumer.class);
  
  private final List<AsyncDataConsumer<D>> _consumerList;
  private final DataConsumer<D> _innerConsumer;
  public CompositeAsyncDataConsumer(ZoieSystem<?,D> zoie,List<AsyncDataConsumer<D>> consumerList) {
    super(zoie.getVersionComparator());
    _consumerList = consumerList;
    if (consumerList!=null && !consumerList.isEmpty()){
      _innerConsumer = new CompositeDataConsumer<D>(zoie.getVersionComparator());
      for (AsyncDataConsumer<D> consumer : consumerList){
        ((CompositeDataConsumer<D>)_innerConsumer).addDataConsumer(consumer);
      }
      zoie.addIndexingEventListener(this);
    }
    else{
      _innerConsumer = zoie;
    }
    
    this.setDataConsumer(_innerConsumer);
  }

  @Override
  public void handleIndexingEvent(IndexingEvent evt) {
    try {
      _innerConsumer.flushEvents();
    } catch (ZoieException e) {
      logger.error(e.getMessage(),e);
    }
  }

  @Override
  public void handleUpdatedDiskVersion(String version) {
    
  }
}
