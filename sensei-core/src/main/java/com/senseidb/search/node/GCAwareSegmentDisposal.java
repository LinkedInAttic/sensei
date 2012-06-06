package com.senseidb.search.node;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;
import org.mortbay.log.Log;

import proj.zoie.api.ZoieSegmentReader;
import proj.zoie.hourglass.impl.AbstractGCAwareSegmentDisposal;

import com.browseengine.bobo.api.BoboIndexReader;

public class GCAwareSegmentDisposal extends AbstractGCAwareSegmentDisposal{
  private static final int LENGTH_THRESHOLD = 20000;
  private static Logger log = Logger.getLogger(GCAwareSegmentDisposal.class);
  private BlockingQueue<Object> disposalQueue = new LinkedBlockingQueue<Object>();  
  private final int delayedGCPerIndexReaderInSeconds;  
  private ExecutorService executor = Executors.newSingleThreadExecutor();  
  class PoisonPill extends Object {
    
  }
  public GCAwareSegmentDisposal(int delayedGCPerIndexReaderinSeconds) {
    this.delayedGCPerIndexReaderInSeconds = delayedGCPerIndexReaderinSeconds; 
    executor.submit(new Runnable() {      
      @Override
      public void run() {
        try {
        while (true) {
            Object obj = disposalQueue.take();
            if (obj instanceof PoisonPill) {
              break;
            }
            log.debug("Object disposed - " + obj.toString());
            synchronized (GCAwareSegmentDisposal.this) {
              GCAwareSegmentDisposal.this.wait(delayedGCPerIndexReaderInSeconds * 1000);
            }
        } 
        } catch (Exception e) {
          Log.warn(e.getMessage(), e);
        }
      }
    });
  }

  @Override
  public void onDelete(ZoieSegmentReader segmentReader, long[] uidArray) {
    if (uidArray.length > LENGTH_THRESHOLD) {
      BoboIndexReader indexReader = (BoboIndexReader) segmentReader.getDecoratedReader();
      disposalQueue.offer(uidArray);
      for (String facetName : indexReader.getFacetNames()) {        
        Object facetData = indexReader.getFacetData(facetName);
        if (facetData != null && !disposalQueue.contains(facetData)) {
          disposalQueue.offer(facetData);
        }        
      }
    }
  }

  @Override
  public void shutdown() {
    disposalQueue.clear();
    disposalQueue.offer(new PoisonPill());
    synchronized (this) {
     this.notifyAll();;
    }
    executor.shutdown();
  }

}
