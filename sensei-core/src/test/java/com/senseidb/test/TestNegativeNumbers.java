package com.senseidb.test;

import java.util.List;

import junit.framework.TestCase;

import org.apache.log4j.Logger;
import org.apache.lucene.search.SortField;

import scala.actors.threadpool.Arrays;

import com.browseengine.bobo.api.BrowseFacet;
import com.browseengine.bobo.api.BrowseSelection;
import com.browseengine.bobo.api.BrowseSelection.ValueOperation;
import com.browseengine.bobo.api.FacetSpec;
import com.browseengine.bobo.api.FacetSpec.FacetSortSpec;
import com.senseidb.search.node.SenseiBroker;
import com.senseidb.search.req.SenseiRequest;
import com.senseidb.search.req.SenseiResult;
import com.senseidb.svc.api.SenseiService;

public class TestNegativeNumbers extends TestCase {

  private static final Logger logger = Logger.getLogger(TestSensei.class);

  private static SenseiBroker broker;
  private static SenseiService httpRestSenseiService;
  static {
    SenseiStarter.start("test-conf/node1","test-conf/node2");
    broker = SenseiStarter.broker;
    httpRestSenseiService = SenseiStarter.httpRestSenseiService;

  }
  public void testSortByAsc() throws Exception {  
      SenseiRequest req = new SenseiRequest();
      String field = "groupid";
      req.setCount(11);
      req.addSortField(new SortField("groupid", SortField.LONG, false));
      SenseiResult res = broker.browse(req);
      long[] groupdIDs = extractFieldValues(field, res);
      assertTrue(Arrays.toString(groupdIDs) + " is not the expected output", Arrays.equals(new long[] {-15000L, -14000L, -13000L, -12000L, -11000L, -10000L, -9000L, -8000L, -7000L, 0L, 10L}, groupdIDs));
  }
  public void test2SortDesc() throws Exception {
    SenseiRequest req = new SenseiRequest();
    String field = "groupid";
    req.setCount(20);
    req.setOffset(14989);
    req.addSortField(new SortField("groupid", SortField.LONG, true));
    SenseiResult res = broker.browse(req);
    long[] groupdIDs = extractFieldValues(field, res);
    assertTrue(Arrays.toString(groupdIDs) + " is not the expected output", Arrays.equals(new long[] {10L, 0L, -7000L, -8000L, -9000L, -10000L, -11000L, -12000L, -13000, -14000, -15000 }, groupdIDs));
  }
  public void test3SortDescWithTerms() throws Exception {
    SenseiRequest req = new SenseiRequest();
    String field = "groupid";
    req.setCount(4);
    
    req.addSortField(new SortField("groupid", SortField.LONG, false));
    req.addSelection(new BrowseSelection("groupid").addValue("10").addValue("0").addValue("-7000").addValue("-8000").setSelectionOperation(ValueOperation.ValueOperationOr));
    req.setFacetSpec("groupid", new FacetSpec().setMaxCount(50).setMinHitCount(1));
    SenseiResult res = broker.browse(req);
    System.out.println(res);
    long[] groupdIDs = extractFieldValues(field, res);
    assertTrue(Arrays.toString(groupdIDs) + " is not the expected output", Arrays.equals(new long[] {-8000L,  -7000L,  0L, 10L}, groupdIDs));
  }
  
  public void test4Facets() throws Exception {
    SenseiRequest req = new SenseiRequest();
    FacetSpec fs = new FacetSpec();
    fs.setMinHitCount(1);
    fs.setOrderBy(FacetSortSpec.OrderValueAsc);
    req.setFacetSpec("groupid", fs);
    SenseiResult res = broker.browse(req);
    List<BrowseFacet> facets = res.getFacetAccessor("groupid").getFacets();
    assertEquals("-0000000000000000000000000000000000015000", facets.get(0).getValue());
    assertEquals(1, facets.get(0).getFacetValueHitCount());
    assertEquals("0000000000000000000000000000000000000000", facets.get(9).getValue());
    assertEquals(1, facets.get(9).getFacetValueHitCount());
    assertEquals("0000000000000000000000000000000000000010", facets.get(10).getValue());
    assertEquals(10, facets.get(10).getFacetValueHitCount());
    for (BrowseFacet facet : facets) {
      if (!facet.getValue().startsWith("-00") && !facet.getValue().startsWith("00")) {
        fail(facet.getValue() + " doesn't start with padding zero");
      }
    }
  }
  public void test5Range() throws Exception {
    SenseiRequest req = new SenseiRequest();
    BrowseSelection sel = new BrowseSelection("groupid_range");
    String selVal = "[* TO 10]";
    sel.addValue(selVal);
    req.addSelection(sel);
    
    
    String field = "groupid_range";
    FacetSpec fs = new FacetSpec();
    fs.setMinHitCount(1);
    fs.setOrderBy(FacetSortSpec.OrderValueAsc);
    req.setFacetSpec(field, fs);
    req.setCount(11);
    req.setOffset(0);
    req.addSortField(new SortField("groupid_range", SortField.LONG, false));
    SenseiResult res = broker.browse(req);
    System.out.println(res);
    long[] groupdIDs = extractFieldValues(field, res);
    assertTrue(Arrays.toString(groupdIDs) + " is not the expected output", Arrays.equals(new long[] {-15000L, -14000L, -13000L, -12000L, -11000L, -10000L, -9000L, -8000L, -7000L, 0L, 10L}, groupdIDs));
  }
  public void test6RangeFacets() throws Exception {
    SenseiRequest req = new SenseiRequest();
    BrowseSelection sel = new BrowseSelection("groupid_range");
    String selVal = "[* TO 10]";
    sel.addValue(selVal);
    req.addSelection(sel);
    
    
    String field = "groupid_range";
    FacetSpec fs = new FacetSpec();
    fs.setMinHitCount(1);
    fs.setOrderBy(FacetSortSpec.OrderValueAsc);
    req.setFacetSpec(field, fs);
    req.setCount(11);
    req.setOffset(0);
    req.addSortField(new SortField("groupid_range", SortField.LONG, false));
    SenseiResult res = broker.browse(req);
    System.out.println(res);
    List<BrowseFacet> facets = res.getFacetAccessor(field).getFacets();
    assertEquals("[* TO -12000]", facets.get(0).getValue());
    assertEquals(4, facets.get(0).getFacetValueHitCount());
    assertEquals("[* TO 10]", facets.get(1).getValue());
    assertEquals(20, facets.get(1).getFacetValueHitCount());    
  }
  public void test7MultiFacets() throws Exception {
    SenseiRequest req = new SenseiRequest();
    FacetSpec fs = new FacetSpec();
    fs.setMinHitCount(1);
    fs.setMaxCount(20);
    fs.setOrderBy(FacetSortSpec.OrderValueAsc);
    req.setFacetSpec("groupid_multi", fs);
    SenseiResult res = broker.browse(req);
    System.out.println(res);
    List<BrowseFacet> facets = res.getFacetAccessor("groupid_multi").getFacets();
    assertEquals("-0000000000000000000000000000000000000500", facets.get(0).getValue());
    assertEquals(2, facets.get(0).getFacetValueHitCount());
    assertEquals("-0000000000000000000000000000000000000200", facets.get(1).getValue());
    assertEquals(2, facets.get(1).getFacetValueHitCount());
    assertEquals("-0000000000000000000000000000000000000001", facets.get(2).getValue());
    assertEquals(3, facets.get(2).getFacetValueHitCount());
    assertEquals("0000000000000000000000000000000000000000", facets.get(3).getValue());
    assertEquals(2, facets.get(3).getFacetValueHitCount());
    assertEquals("0000000000000000000000000000000000000001", facets.get(4).getValue());
    assertEquals(1, facets.get(4).getFacetValueHitCount());
    assertEquals("0000000000000000000000000000000000000500", facets.get(5).getValue());
    assertEquals(1, facets.get(5).getFacetValueHitCount());
  }
  public void test8MultiTerm() throws Exception {
    SenseiRequest req = new SenseiRequest();
    req.setCount(100);
    String fieldName = "groupid_multi";
    req.addSelection(new BrowseSelection(fieldName).addValue("-1").addValue("1").addNotValue("-500").setSelectionOperation(ValueOperation.ValueOperationOr));
    req.addSortField(new SortField(fieldName, SortField.LONG, false));
    req.setFacetSpec(fieldName, new FacetSpec().setMaxCount(50).setMinHitCount(1).setOrderBy(FacetSortSpec.OrderValueAsc));
    SenseiResult res = broker.browse(req);
    assertEquals(1, res.getNumHits());
    
  }
  private long[] extractFieldValues(String field, SenseiResult res) {
    long[] groupdIDs = new long[res.getSenseiHits().length];
    for (int i = 0; i < res.getSenseiHits().length; i++) {
      groupdIDs[i] = Long.parseLong(res.getSenseiHits()[i].getFieldValues().get(field)[0]);
    }
    return groupdIDs;
  }
}
