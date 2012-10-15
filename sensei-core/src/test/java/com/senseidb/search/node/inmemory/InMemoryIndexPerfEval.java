package com.senseidb.search.node.inmemory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.json.JSONObject;

import com.browseengine.bobo.api.BrowseSelection;
import com.browseengine.bobo.api.FacetSpec;
import com.browseengine.bobo.api.FacetSpec.FacetSortSpec;
import com.senseidb.search.req.SenseiRequest;

public class InMemoryIndexPerfEval {

  public static void main(String[] args) throws Exception {
    final InMemorySenseiService memorySenseiService = InMemorySenseiService.valueOf(new File(
        InMemoryIndexPerfEval.class.getClassLoader().getResource("test-conf/node1/").toURI()));
        
      
    final List<JSONObject> docs = new ArrayList<JSONObject>(15000);
    LineIterator lineIterator = FileUtils.lineIterator(new File(InMemoryIndexPerfEval.class.getClassLoader().getResource("data/test_data.json").toURI()));
    int i = 0;
    while(lineIterator.hasNext() && i < 100) {
      String car = lineIterator.next();
      if (car != null && car.contains("{"))
      docs.add(new JSONObject(car));
      i++;
    }
    Thread[] threads = new Thread[10];
    for (int k = 0 ; k < threads.length; k++) {
      threads[k] = new Thread(new Runnable() {  
        public void run() {
          long time = System.currentTimeMillis();
          //System.out.println("Start thread");
          for (int j = 0; j < 1000; j++) {
            //System.out.println("Send request");
            memorySenseiService.doQuery(getRequest(), docs);      
          }          
          System.out.println("time = " + (System.currentTimeMillis() - time));
        }
      }); 
      threads[k].start();
    }    
    Thread.sleep(500000);
  }
  private static void setspec(SenseiRequest req, FacetSpec spec) {
    req.setFacetSpec("color", spec);
    req.setFacetSpec("category", spec);
    req.setFacetSpec("city", spec);
    req.setFacetSpec("makemodel", spec);
    req.setFacetSpec("year", spec);
    req.setFacetSpec("price", spec);
    req.setFacetSpec("mileage", spec);
    req.setFacetSpec("tags", spec);
  }
  public static SenseiRequest getRequest() {
    FacetSpec facetSpecall = new FacetSpec();
    facetSpecall.setMaxCount(1000000);
    facetSpecall.setExpandSelection(true);
    facetSpecall.setMinHitCount(0);
    facetSpecall.setOrderBy(FacetSortSpec.OrderHitsDesc);
    FacetSpec facetSpec = new FacetSpec();
    facetSpec.setMaxCount(5);
    SenseiRequest req = new SenseiRequest();
    req.setCount(3);
    facetSpecall.setMaxCount(3);
    setspec(req, facetSpecall);
    BrowseSelection sel = new BrowseSelection("year");
    String selVal = "[2001 TO 2002]";
    sel.addValue(selVal);
    req.addSelection(sel);
    return req;
  }
}
