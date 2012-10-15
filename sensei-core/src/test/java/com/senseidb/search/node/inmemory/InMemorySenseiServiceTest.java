package com.senseidb.search.node.inmemory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.json.JSONObject;

import com.senseidb.search.req.SenseiResult;

import junit.framework.TestCase;

public class InMemorySenseiServiceTest extends TestCase {
  private InMemorySenseiService inMemorySenseiService;
  private List<JSONObject> docs;
  @Override
  protected void setUp() throws Exception {
    inMemorySenseiService =  InMemorySenseiService.valueOf(new File(
        InMemoryIndexPerfEval.class.getClassLoader().getResource("test-conf/node1/").toURI()));
    LineIterator lineIterator = FileUtils.lineIterator(new File(InMemoryIndexPerfEval.class.getClassLoader().getResource("data/test_data.json").toURI()));
    int i = 0;
    docs = new ArrayList<JSONObject>();
    while(lineIterator.hasNext() && i < 100) {
      String car = lineIterator.next();
      if (car != null && car.contains("{"))
      docs.add(new JSONObject(car));
      i++;
    }
    lineIterator.close();
  }
  public void test1() {
    SenseiResult result = inMemorySenseiService.doQuery(InMemoryIndexPerfEval.getRequest(), docs);
    assertEquals(16, result.getNumHits());
    assertEquals(100, result.getTotalDocs());
  }
}
