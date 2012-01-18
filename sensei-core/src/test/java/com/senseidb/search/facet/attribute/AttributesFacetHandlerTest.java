package com.senseidb.search.facet.attribute;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.store.RAMDirectory;

import com.browseengine.bobo.api.BoboBrowser;
import com.browseengine.bobo.api.BoboIndexReader;
import com.browseengine.bobo.api.BrowseFacet;
import com.browseengine.bobo.api.BrowseRequest;
import com.browseengine.bobo.api.BrowseResult;
import com.browseengine.bobo.api.BrowseSelection;
import com.browseengine.bobo.api.BrowseSelection.ValueOperation;
import com.browseengine.bobo.api.FacetAccessible;
import com.browseengine.bobo.api.FacetSpec;
import com.browseengine.bobo.api.FacetSpec.FacetSortSpec;
import com.browseengine.bobo.facets.FacetCountCollectorSource;
import com.browseengine.bobo.facets.FacetHandler;
import com.browseengine.bobo.facets.filter.RandomAccessFilter;
import com.senseidb.search.facet.attribute.AttributesFacetHandler;

public class AttributesFacetHandlerTest extends TestCase {

  private RAMDirectory directory;
  private Analyzer analyzer;
  private List<FacetHandler<?>> facetHandlers;
  private com.senseidb.search.facet.attribute.AttributesFacetHandler attributesFacetHandler;
  private BoboBrowser browser;
  private BoboIndexReader boboReader;
  private Map<String, String> selectionProperties;
  static final String AttributeHandlerName = "attributes";

  public AttributesFacetHandlerTest(String name) {
    super(name);
    facetHandlers = new LinkedList<FacetHandler<?>>();
  }

  private void addMetaDataField(Document doc, String name, String[] vals) {
    for (String val : vals) {
      Field field = new Field(name, val, Store.NO, Index.NOT_ANALYZED_NO_NORMS);
      field.setOmitTermFreqAndPositions(true);
      doc.add(field);
    }
  }

  @Override
  protected void setUp() throws Exception {
    directory = new RAMDirectory();
    analyzer = new WhitespaceAnalyzer();
    selectionProperties = new HashMap<String, String>();
    IndexWriter writer = new IndexWriter(directory, analyzer, true, MaxFieldLength.UNLIMITED);

    writer.addDocument(doc("prop1=val1", "prop2=val1", "prop5=val1"));
    writer.addDocument(doc("prop1=val2", "prop3=val1", "prop7=val7"));
    writer.addDocument(doc("prop1=val2", "prop3=val2", "prop3=val3"));
    writer.addDocument(doc("prop1=val1", "prop2=val1"));
    writer.addDocument(doc("prop1=val1", "prop2=val1"));
    writer.addDocument(doc("prop1=val1", "prop2=val1", "prop4=val2", "prop4=val3"));
    writer.commit();

    attributesFacetHandler = new AttributesFacetHandler(AttributeHandlerName, AttributeHandlerName, null, null,
        Collections.EMPTY_SET, new HashMap<String, String>());
    facetHandlers.add(attributesFacetHandler);
    IndexReader reader = IndexReader.open(directory, true);
     boboReader = BoboIndexReader.getInstance(reader, facetHandlers);
    attributesFacetHandler.loadFacetData(boboReader);
    browser = new BoboBrowser(boboReader);
  }

  private Document doc(String... terms) {
    Document doc = new Document();
    addMetaDataField(doc, AttributeHandlerName, terms);
    return doc;
  }
  public BrowseRequest createRequest(int minHitCount,  String... terms) {
    return createRequest(minHitCount, ValueOperation.ValueOperationOr, terms);
  }
  public BrowseRequest createRequest(int minHitCount, BrowseSelection.ValueOperation operation, String... terms) {
    BrowseRequest req = new BrowseRequest();

    BrowseSelection sel = new BrowseSelection(AttributeHandlerName);
    for (String term : terms) {
      sel.addValue(term);
    }    
    sel.setSelectionProperties(selectionProperties);
    sel.setSelectionOperation(operation);
    req.addSelection(sel);
    req.setCount(50);
    FacetSpec fs = new FacetSpec();
    fs.setMinHitCount(minHitCount);
    req.setFacetSpec(AttributeHandlerName, fs);
    return req;
  }
  public void test10ModifiedNumberOfFacetsPerKeyInSelection() throws Exception {
    modifiedSetup();
    selectionProperties.put(AttributesFacetHandler.MAX_FACETS_PER_KEY_PROP_NAME, "2");
    BrowseRequest request = createRequest(1, ValueOperation.ValueOperationOr, "prop1", "prop2", "prop3", "prop4", "prop5", "prop6", "prop7");
    request.getFacetSpec(AttributeHandlerName).setOrderBy(FacetSortSpec.OrderHitsDesc);
    BrowseResult res = browser.browse(request);   
    System.out.println(res);    
    List<BrowseFacet> facets = res.getFacetAccessor(AttributeHandlerName).getFacets();
    assertEquals(facets.size(), 9);
    assertEquals(facets.get(0).getValue(), "prop1=val1");
    assertEquals(facets.get(0).getFacetValueHitCount(), 4);    
    assertEquals(facets.get(1).getValue(), "prop2=val1");
    assertEquals(facets.get(1).getFacetValueHitCount(), 4);
    assertEquals(facets.get(2).getValue(), "prop1=val2");
    assertEquals(facets.get(2).getFacetValueHitCount(), 2);
    assertEquals(facets.get(3).getValue(), "prop3=val1");
    assertEquals(facets.get(3).getFacetValueHitCount(), 1);  
  }
  public void test8AndPropertiesPlsExclusion() throws Exception {
    BrowseRequest request = createRequest(1, ValueOperation.ValueOperationAnd, "prop1", "prop3");
    request.getSelection(AttributeHandlerName).addNotValue("prop7");
    BrowseResult res = browser.browse(request);   
    System.out.println(res);
    assertEquals(res.getNumHits(), 1);
    assertEquals(res.getHits()[0].getDocid(), 2);
    List<BrowseFacet> facets = res.getFacetAccessor(AttributeHandlerName).getFacets();
    assertEquals(facets.size(), 3);
    assertEquals(facets.get(0).getValue(), "prop1=val2");
    assertEquals(facets.get(0).getFacetValueHitCount(), 1);    
    assertEquals(facets.get(1).getValue(), "prop3=val2");
    assertEquals(facets.get(1).getFacetValueHitCount(), 1);  
    assertEquals(facets.get(2).getValue(), "prop3=val3");
    assertEquals(facets.get(2).getFacetValueHitCount(), 1);  
  }
  
  
 
  public void test1Filter() throws Exception {
    BrowseRequest request = createRequest(1, "prop3");
    FacetCountCollectorSource facetCountCollectorSource = attributesFacetHandler.getFacetCountCollectorSource(request.getSelection(AttributeHandlerName), request.getFacetSpec(AttributeHandlerName));
    RandomAccessFilter randomAccessFilter = attributesFacetHandler.buildFilter(request.getSelection(AttributeHandlerName));
    DocIdSetIterator iterator = randomAccessFilter.getDocIdSet(boboReader).iterator();
    int docId = iterator.nextDoc();
    int[] docIds = new int[2];
    int i = 0;
    while (docId != iterator.NO_MORE_DOCS) {
      docIds[i] = docId;
      i++;
      docId = iterator.nextDoc();
    }
    assertEquals(Arrays.toString(new int[] {1,2}), Arrays.toString(docIds));
  
    BrowseResult res = browser.browse(request);
    assertEquals(res.getNumHits(), 2);
    FacetAccessible fa = res.getFacetAccessor(AttributeHandlerName);
    List<BrowseFacet> facets = fa.getFacets();
    System.out.println(facets);
    assertEquals(3, facets.size());
    BrowseFacet facet = facets.get(0);
    assertEquals(1, facet.getFacetValueHitCount());
  }
  public void test2PropertyRetrieval() throws Exception {
    BrowseRequest request = createRequest(1, "prop3");
    BrowseResult res = browser.browse(request);
    assertEquals(res.getNumHits(), 2);
    assertEquals(res.getHits()[0].getDocid(), 1);
    assertEquals(res.getHits()[1].getDocid(), 2);
    List<BrowseFacet> facets = res.getFacetAccessor(AttributeHandlerName).getFacets();
    assertEquals(facets.size(), 3);
    assertEquals(facets.get(0).getValue(), "prop3=val1");
    assertEquals(facets.get(0).getFacetValueHitCount(), 1);
    assertEquals(facets.get(2).getValue(), "prop3=val3");
    assertEquals(facets.get(2).getFacetValueHitCount(), 1);
  }
  public void test3PropertyInEachDocRetrieval() throws Exception {
    BrowseRequest request = createRequest(1, "prop1");
    BrowseResult res = browser.browse(request);
    assertEquals(res.getNumHits(), 6);
    assertEquals(res.getHits()[0].getDocid(), 0);
    assertEquals(res.getHits()[5].getDocid(), 5);
    List<BrowseFacet> facets = res.getFacetAccessor(AttributeHandlerName).getFacets();
    assertEquals(facets.size(), 2);
    assertEquals(facets.get(0).getValue(), "prop1=val1");
    assertEquals(facets.get(0).getFacetValueHitCount(), 4);
    assertEquals(facets.get(1).getValue(), "prop1=val2");
    assertEquals(facets.get(1).getFacetValueHitCount(), 2);
  }
  public void test4PropertyInFirstDocRetrieval() throws Exception {
    BrowseRequest request = createRequest(1, "prop5");
    BrowseResult res = browser.browse(request);
    assertEquals(res.getNumHits(), 1);
    assertEquals(res.getHits()[0].getDocid(), 0);
   
    List<BrowseFacet> facets = res.getFacetAccessor(AttributeHandlerName).getFacets();
    assertEquals(facets.size(), 1);
    assertEquals(facets.get(0).getValue(), "prop5=val1");
    assertEquals(facets.get(0).getFacetValueHitCount(), 1);    
  }
  public void test5PropertyInLastDocRetrieval() throws Exception {
    BrowseRequest request = createRequest(1, "prop4");
    BrowseResult res = browser.browse(request);
    System.out.println(res);
    assertEquals(res.getNumHits(), 1);
    assertEquals(res.getHits()[0].getDocid(), 5);
   
    List<BrowseFacet> facets = res.getFacetAccessor(AttributeHandlerName).getFacets();
    assertEquals(facets.size(), 2);
    assertEquals(facets.get(0).getValue(), "prop4=val2");
    assertEquals(facets.get(0).getFacetValueHitCount(), 1);    
    assertEquals(facets.get(1).getValue(), "prop4=val3");
    assertEquals(facets.get(1).getFacetValueHitCount(), 1);    
  }
 
  public void test6NonExisitngPropertyDocRetrieval() throws Exception {
    BrowseRequest request = createRequest(1, "propMissing");
    BrowseResult res = browser.browse(request);
    assertEquals(res.getNumHits(), 0);   
  }
  public void test7AndProperties() throws Exception {
    BrowseRequest request = createRequest(1, ValueOperation.ValueOperationAnd, "prop1", "prop3");
    BrowseResult res = browser.browse(request);   
    System.out.println(res);
    assertEquals(res.getNumHits(), 2);
    assertEquals(res.getHits()[0].getDocid(), 1);
    assertEquals(res.getHits()[1].getDocid(), 2);
    List<BrowseFacet> facets = res.getFacetAccessor(AttributeHandlerName).getFacets();
    assertEquals(facets.size(), 4);
    assertEquals(facets.get(0).getValue(), "prop1=val2");
    assertEquals(facets.get(0).getFacetValueHitCount(), 2);    
    assertEquals(facets.get(1).getValue(), "prop3=val1");
    assertEquals(facets.get(1).getFacetValueHitCount(), 1);  
  }
  
  public void test9ModifiedNumberOfFacetsPerKey() throws Exception {
    modifiedSetup();
    BrowseRequest request = createRequest(1, ValueOperation.ValueOperationOr);
    request.getFacetSpec(AttributeHandlerName).setOrderBy(FacetSortSpec.OrderHitsDesc);
    BrowseResult res = browser.browse(request);   
    System.out.println(res);    
    List<BrowseFacet> facets = res.getFacetAccessor(AttributeHandlerName).getFacets();
    assertEquals(facets.size(), 6);
    assertEquals(facets.get(0).getValue(), "prop1=val1");
    assertEquals(facets.get(0).getFacetValueHitCount(), 4);    
    assertEquals(facets.get(1).getValue(), "prop2=val1");
    assertEquals(facets.get(1).getFacetValueHitCount(), 4);  
    assertEquals(facets.get(2).getValue(), "prop3=val1");
    assertEquals(facets.get(2).getFacetValueHitCount(), 1);  
  }
  

  private void modifiedSetup() throws CorruptIndexException, LockObtainFailedException, IOException {
    directory = new RAMDirectory();
    analyzer = new WhitespaceAnalyzer();
    IndexWriter writer = new IndexWriter(directory, analyzer, true, MaxFieldLength.UNLIMITED);

    writer.addDocument(doc("prop1=val1", "prop2=val1", "prop5=val1"));
    writer.addDocument(doc("prop1=val2", "prop3=val1", "prop7=val7"));
    writer.addDocument(doc("prop1=val2", "prop3=val2", "prop3=val3"));
    writer.addDocument(doc("prop1=val1", "prop2=val1"));
    writer.addDocument(doc("prop1=val1", "prop2=val1"));
    writer.addDocument(doc("prop1=val1", "prop2=val1", "prop4=val2", "prop4=val3"));
    writer.commit();

    HashMap<String, String> facetProps = new HashMap<String, String>();
    facetProps.put(AttributesFacetHandler.MAX_FACETS_PER_KEY_PROP_NAME, "1");
    attributesFacetHandler = new AttributesFacetHandler(AttributeHandlerName, AttributeHandlerName, null, null,
        Collections.EMPTY_SET, facetProps);
    facetHandlers.add(attributesFacetHandler);
    IndexReader reader = IndexReader.open(directory, true);
     boboReader = BoboIndexReader.getInstance(reader, facetHandlers);
    attributesFacetHandler.loadFacetData(boboReader);
    browser = new BoboBrowser(boboReader);
  }
  
}
