package com.senseidb.test;


import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import junit.framework.TestCase;

import org.apache.commons.configuration.DataConfiguration;
import org.apache.commons.configuration.web.ServletRequestConfiguration;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.SortField;
import org.json.JSONException;
import org.json.JSONObject;

import com.browseengine.bobo.api.BrowseFacet;
import com.browseengine.bobo.api.BrowseSelection;
import com.browseengine.bobo.api.FacetAccessible;
import com.browseengine.bobo.api.FacetSpec;
import com.browseengine.bobo.api.MappedFacetAccessible;
import com.browseengine.bobo.facets.DefaultFacetHandlerInitializerParam;
import com.browseengine.bobo.facets.FacetHandlerInitializerParam;
import com.senseidb.search.req.SenseiHit;
import com.senseidb.search.req.SenseiJSONQuery;
import com.senseidb.search.req.SenseiQuery;
import com.senseidb.search.req.SenseiRequest;
import com.senseidb.search.req.SenseiResult;
import com.senseidb.servlet.DefaultSenseiJSONServlet;
import com.senseidb.svc.api.SenseiException;
import com.senseidb.svc.impl.HttpRestSenseiServiceImpl;
import com.senseidb.util.JSONUtil.FastJSONArray;
import com.senseidb.util.JSONUtil.FastJSONObject;


public class TestHttpRestSenseiServiceImpl extends TestCase
{
  private static final int EXPECTED_COUNT = 72;
  private static  final int EXPECTED_OFFSET = 227;
  private static  final boolean EXPECTED_FETCH_STORED_FIELDS = true;
  private static  final boolean EXPECTED_SHOW_EXPLANATION = true;

  public TestHttpRestSenseiServiceImpl(String name)
  {
    super(name);
  }

  public void testSenseiResultParsing()
      throws Exception
  {
    SenseiRequest aRequest = createNonRandomSenseiRequest();
    SenseiResult aResult = createMockResultFromRequest(aRequest);
    JSONObject resultJSONObj = DefaultSenseiJSONServlet.buildJSONResult(aRequest, aResult);
    SenseiResult bResult = HttpRestSenseiServiceImpl.buildSenseiResult(resultJSONObj);
    assertEquals(aResult, bResult);
  }

  private SenseiResult createMockResultFromRequest(SenseiRequest request) {
    SenseiResult result = new SenseiResult();

    result.setParsedQuery("This is my parsed query value");
    result.setTime(Long.MAX_VALUE / 2);
    result.setNumHits(Integer.MAX_VALUE / 2);
    result.setTid(1);
    result.setTotalDocs(512);
    result.setHits(createSenseiHits(10));
    result.addAll(createFacetAccessibleMap(request));

    return result;
  }

  private SenseiHit[] createSenseiHits(int count) {
    List<SenseiHit> hits = new ArrayList<SenseiHit>();

    for (int i = 0; i < count; i++) {
      SenseiHit sh = new SenseiHit();
      sh.setUID(i);
      sh.setDocid(100 + i);
      sh.setExplanation(createExplanation(i, i));
      sh.setFieldValues(i % 2 == 0 ? createFieldValues(i) : null);
      sh.setRawFieldValues(i % 2 == 0 ? createRawFieldValues(i) : null);
      sh.setStoredFields(createSenseiHitDocument());
      hits.add(sh);
    }

    return hits.toArray(new SenseiHit[hits.size()]);
  }

  private Document createSenseiHitDocument() {
    Document doc = new Document();

    for (int i = 0; i < 10; i++) {
      doc.add(new org.apache.lucene.document.Field("name" + i, "value" + i, Field.Store.YES, Field.Index.ANALYZED));
    }

    return doc;
  }

  private Map<String,String[]> createFieldValues(int uid) {
    Map<String,String[]> map = new HashMap<String,String[]>();

    for (int i = 0; i < 10; i++) {
      map.put(String.format("key %s %s", uid, i), new String[]{"hello" + i, "world" + i});
    }

    return map;
  }

  private Map<String,Object[]> createRawFieldValues(int uid) {
    Map<String,Object[]> map = new HashMap<String,Object[]>();

    for (int i = 0; i < 10; i++) {
      map.put(String.format("raw key %s %s", uid, i), new String[] { "hello" + i, "world" + i} );
    }

    return map;
  }

  private Explanation createExplanation(int facetIndex, int descCount) {
    Explanation expl = new Explanation();

    expl.setDescription(String.format("facetIndex = %s, and this is my description", facetIndex));
    expl.setValue(facetIndex);

    for (int i = 0; i < descCount; i++) {
      expl.addDetail(createExplanation((1000 * facetIndex) + i, 0));
    }

    return expl;
  }

  private Map<String, FacetAccessible> createFacetAccessibleMap(SenseiRequest request) {
    Map<String, FacetAccessible> facetAccessibleMap = new HashMap<String, FacetAccessible>();

    for (int i = 10; i < 20; i++) {
      String fieldName = "fieldName_" + i;

      List<BrowseFacet> bfList = new ArrayList<BrowseFacet>();

      Map<String,FacetSpec> facetSpecs = request.getFacetSpecs();

      // copy the requests facets over
      for (String facetName : facetSpecs.keySet()) {
        BrowseFacet bf = new BrowseFacet();
        bf.setFacetValueHitCount(i);
        bf.setValue(String.format("fieldName %s, value = %s", fieldName, facetName));
        bfList.add(bf);
      }

      MappedFacetAccessible mfa = new MappedFacetAccessible(bfList.toArray(new BrowseFacet[bfList.size()]));

      facetAccessibleMap.put(fieldName, mfa);
    }

    return facetAccessibleMap;
  }

  public void testURIBuilding()
      throws JSONException, SenseiException, UnsupportedEncodingException, URISyntaxException, MalformedURLException
  {
    SenseiRequest aRequest = createNonRandomSenseiRequest();
    List<NameValuePair> queryParams = HttpRestSenseiServiceImpl.convertRequestToQueryParams(aRequest);
    HttpRestSenseiServiceImpl senseiService = createSenseiService();
    URI requestURI = senseiService.buildRequestURI(queryParams);

    assertTrue(requestURI.toURL().toString().length() > 0); // force resolving the URI to a string

    List<NameValuePair> parsedParams = URLEncodedUtils.parse(requestURI, "UTF-8");
    MockServletRequest mockServletRequest = MockServletRequest.create(parsedParams);
    DataConfiguration params = new DataConfiguration(new ServletRequestConfiguration(mockServletRequest));
    SenseiRequest bRequest = DefaultSenseiJSONServlet.convertSenseiRequest(params);
    assertEquals(aRequest, bRequest);
  }

  public void testConvertSenseiRequest()
      throws SenseiException, UnsupportedEncodingException, JSONException
  {
    SenseiRequest testRequest = createNonRandomSenseiRequest();
    List<NameValuePair> list = HttpRestSenseiServiceImpl.convertRequestToQueryParams(testRequest);
    MockServletRequest mockServletRequest = MockServletRequest.create(list);
    DataConfiguration params = new DataConfiguration(new ServletRequestConfiguration(mockServletRequest));
    SenseiRequest resultRequest = DefaultSenseiJSONServlet.convertSenseiRequest(params);
    assertEquals(testRequest, resultRequest);
  }

  public void testConvertScalarValues()
      throws SenseiException, UnsupportedEncodingException, JSONException
  {
    SenseiRequest aRequest = new SenseiRequest();

    aRequest.setCount(EXPECTED_COUNT);
    aRequest.setOffset(EXPECTED_OFFSET);
    aRequest.setFetchStoredFields(EXPECTED_FETCH_STORED_FIELDS);
    aRequest.setShowExplanation(EXPECTED_SHOW_EXPLANATION);

    SenseiRequest bRequest = new SenseiRequest();
    List<NameValuePair> list = HttpRestSenseiServiceImpl.convertRequestToQueryParams(aRequest);
    MockServletRequest mockServletRequest = MockServletRequest.create(list);
    DataConfiguration params = new DataConfiguration(new ServletRequestConfiguration(mockServletRequest));
    DefaultSenseiJSONServlet.convertScalarParams(bRequest, params);
    assertEquals(aRequest, bRequest);
  }

  public void testInitParams()
      throws UnsupportedEncodingException
  {
    SenseiRequest aRequest = new SenseiRequest();
    Map<String, FacetHandlerInitializerParam> initParams = createInitParams();
    aRequest.putAllFacetHandlerInitializerParams(initParams);

    SenseiRequest bRequest = new SenseiRequest();
    List<NameValuePair> qparams = new ArrayList<NameValuePair>();
    HttpRestSenseiServiceImpl.convertFacetInitParams(qparams, initParams);
    MockServletRequest mockServletRequest = MockServletRequest.create(qparams);
    DataConfiguration params = new DataConfiguration(new ServletRequestConfiguration(mockServletRequest));
    DefaultSenseiJSONServlet.convertInitParams(bRequest, params);
    assertEquals(aRequest, bRequest);
  }

  public void testFacetSpecs()
  {
    SenseiRequest aRequest = new SenseiRequest();
    Map<String, FacetSpec> facetSpecMap = createFacetSpecMap();
    aRequest.setFacetSpecs(facetSpecMap);

    SenseiRequest bRequest = new SenseiRequest();
    List<NameValuePair> list = new ArrayList<NameValuePair>();
    HttpRestSenseiServiceImpl.convertFacetSpecs(list, facetSpecMap);
    MockServletRequest mockServletRequest = MockServletRequest.create(list);
    DataConfiguration params = new DataConfiguration(new ServletRequestConfiguration(mockServletRequest));
    DefaultSenseiJSONServlet.convertFacetParam(bRequest, params);
    assertEquals(aRequest, bRequest);
  }

  public void testSortFields()
  {
    SenseiRequest aRequest = new SenseiRequest();
    final SortField[] sortFields = createSortFields();
    aRequest.addSortFields(sortFields);

    SenseiRequest bRequest = new SenseiRequest();
    List<NameValuePair> list = new ArrayList<NameValuePair>();
    HttpRestSenseiServiceImpl.convertSortFieldParams(list, sortFields);
    MockServletRequest mockServletRequest = MockServletRequest.create(list);
    DataConfiguration params = new DataConfiguration(new ServletRequestConfiguration(mockServletRequest));
    DefaultSenseiJSONServlet.convertSortParam(bRequest, params);
    assertEquals(aRequest, bRequest);
  }

  public void testSenseiQuery()
      throws SenseiException, JSONException
  {
    SenseiRequest aRequest = new SenseiRequest();
    SenseiQuery senseiQuery = createSenseiQuery();
    aRequest.setQuery(senseiQuery);

    SenseiRequest bRequest = new SenseiRequest();
    List<NameValuePair> list = new ArrayList<NameValuePair>();
    HttpRestSenseiServiceImpl.convertSenseiQuery(list, senseiQuery);
    MockServletRequest mockServletRequest = MockServletRequest.create(list);
    DataConfiguration params = new DataConfiguration(new ServletRequestConfiguration(mockServletRequest));
    DefaultSenseiJSONServlet.convertSenseiQuery(bRequest, params);
    assertEquals(aRequest, bRequest);
  }

  public void testNullSenseiQuery()
      throws SenseiException
  {
    List<NameValuePair> list = new ArrayList<NameValuePair>();
    HttpRestSenseiServiceImpl.convertSenseiQuery(list, null);
    assertTrue(list.size() == 0);
  }

  public void testPartitions()
      throws SenseiException, JSONException
  {
    SenseiRequest aRequest = new SenseiRequest();
    Set<Integer> partitions = createPartitions();
    aRequest.setPartitions(partitions);

    SenseiRequest bRequest = new SenseiRequest();
    List<NameValuePair> list = new ArrayList<NameValuePair>();
    HttpRestSenseiServiceImpl.convertPartitionParams(list, partitions);
    MockServletRequest mockServletRequest = MockServletRequest.create(list);
    DataConfiguration params = new DataConfiguration(new ServletRequestConfiguration(mockServletRequest));
    DefaultSenseiJSONServlet.convertPartitionParams(bRequest, params);
    assertEquals(aRequest, bRequest);
  }

  public void testNullPartitions() {
    List<NameValuePair> list = new ArrayList<NameValuePair>();
    HttpRestSenseiServiceImpl.convertPartitionParams(list, null);
    assertTrue(list.size() == 0);
  }

  public void testBrowseSelections()
      throws JSONException
  {
    SenseiRequest aRequest = new SenseiRequest();
    BrowseSelection[] selections = createBrowseSelections();
    aRequest.addSelections(selections);

    SenseiRequest bRequest = new SenseiRequest();
    bRequest.addSelections(selections);
    List<NameValuePair> list = new ArrayList<NameValuePair>();
    HttpRestSenseiServiceImpl.convertSelectionNames(list, bRequest);
    MockServletRequest mockServletRequest = MockServletRequest.create(list);
    DataConfiguration params = new DataConfiguration(new ServletRequestConfiguration(mockServletRequest));
    DefaultSenseiJSONServlet.convertSelectParam(bRequest, params);
    assertEquals(aRequest, bRequest);
  }

  private HttpRestSenseiServiceImpl createSenseiService() {
    return new HttpRestSenseiServiceImpl(
        "http",
        "localhost",
        80,
        "/sensei",
        2000,
        5,
        null);
  }

  private SenseiRequest createNonRandomSenseiRequest()
      throws JSONException
  {
    SenseiRequest req = new SenseiRequest();

    createScalarValues(req);

    req.setFacetHandlerInitParamMap(createInitParams());
    req.setFacetSpecs(createFacetSpecMap());
    req.setSort(createSortFields());
    req.setQuery(createSenseiQuery());
    req.addSelections(createBrowseSelections());
    req.setPartitions(createPartitions());

    return req;
  }

  void createScalarValues(SenseiRequest req) {
    req.setCount(EXPECTED_COUNT);
    req.setOffset(EXPECTED_OFFSET);
    req.setFetchStoredFields(EXPECTED_FETCH_STORED_FIELDS);
    req.setShowExplanation(EXPECTED_SHOW_EXPLANATION);
  }

  Set<Integer> createPartitions() {
    HashSet<Integer> partitions = new HashSet<Integer>();

    for (int i = 0; i < 10; i++) {
      partitions.add(i*i);
    }

    return partitions;
  }

  BrowseSelection[] createBrowseSelections() {
    List<BrowseSelection> list = new ArrayList<BrowseSelection>();

    BrowseSelection selection;

    selection = new BrowseSelection("aSelection");
    selection.addNotValue("notVal");
    selection.addValue("aVal");
    selection.setSelectionOperation(BrowseSelection.ValueOperation.ValueOperationAnd);

    list.add(selection);

    return list.toArray(new BrowseSelection[list.size()]);
  }

  SenseiQuery createSenseiQuery()
      throws JSONException
  {

    JSONObject obj = new FastJSONObject();

    obj.put("query", "key words are useful");  // 'query' in the JSONObj gets translated into 'q' in the GET request

    for (int i = 0; i < 10; i++) {
      obj.put("key" + i, "val" + i);
    }

    SenseiQuery query = new SenseiJSONQuery(obj);

    return query;
  }

  SortField[] createSortFields() {
    List<SortField> list = new ArrayList<SortField>();

    list.add(new SortField(null, SortField.DOC));
    list.add(new SortField(null, SortField.DOC, true));

    list.add(new SortField(null, SortField.SCORE));
    list.add(new SortField(null, SortField.SCORE, true));

    list.add(new SortField("fieldCUSTOM", SortField.CUSTOM, false));
    list.add(new SortField("fieldCUSTOMREV", SortField.CUSTOM, true));

    return list.toArray(new SortField[list.size()]);
  }

  Map<String, FacetSpec> createFacetSpecMap()
  {
    Map<String, FacetSpec> map = new HashMap<String, FacetSpec>();

    FacetSpec spec = new FacetSpec();
    spec.setExpandSelection(false);
    spec.setMaxCount(10);
    spec.setMinHitCount(2);
    spec.setOrderBy(FacetSpec.FacetSortSpec.OrderHitsDesc);
    map.put("facet1", spec);

    spec = new FacetSpec();
    spec.setExpandSelection(true);
    spec.setMaxCount(5);
    spec.setMinHitCount(10);
    spec.setOrderBy(FacetSpec.FacetSortSpec.OrderValueAsc);
    map.put("facet2", spec);

    for (int i = 3; i < 10; i++) {
      spec = new FacetSpec();
      spec.setExpandSelection(i % 2 == 0);
      spec.setMaxCount(i * 5);
      spec.setMinHitCount(i);
      spec.setOrderBy(i % 2 == 0 ? FacetSpec.FacetSortSpec.OrderValueAsc : FacetSpec.FacetSortSpec.OrderHitsDesc);
      map.put("facet" + i, spec);
    }

/* NOT YET SUPPORTED
    spec = new FacetSpec();
    spec.setExpandSelection(true);
    spec.setMaxCount(50);
    spec.setMinHitCount(9);
    spec.setOrderBy(FacetSpec.FacetSortSpec.OrderByCustom);
    map.put("facet3", spec);
*/

    return map;
  }

  Map<String, FacetHandlerInitializerParam> createInitParams()
  {
    Map<String, FacetHandlerInitializerParam> map = new HashMap<String, FacetHandlerInitializerParam>();

    DefaultFacetHandlerInitializerParam param;

    param = new DefaultFacetHandlerInitializerParam();
    for (int i = 0; i < 2; i++) {
      param.putBooleanParam("boolParam" + i, new boolean[]{false});
      map.put("boolFacet" + i, param);
    }

    param = new DefaultFacetHandlerInitializerParam();
    for (int i = 0; i < 2; i++) {
      param.putIntParam("intParam" + i, new int[]{42});
      map.put("intFacet" + i, param);
    }

/*  NOT YET SUPPORTED
    param = new DefaultFacetHandlerInitializerParam();
    param.putByteArrayParam("bytearrayParam", new byte[]{1, 2, 3});
    map.put("bytearrayFacet", param);
 */

    param = new DefaultFacetHandlerInitializerParam();
    for (int i = 0; i < 2; i++) {
      param.putStringParam("stringParam" + i, new ArrayList<String>(){{ add("woot"); }});
      map.put("stringFacet" + i, param);
    }

    param = new DefaultFacetHandlerInitializerParam();
    for (int i = 0; i < 2; i++) {
      param.putDoubleParam("doubleParam" + i, new double[]{3.141592});
      map.put("doubleFacet" + i, param);
    }

    param = new DefaultFacetHandlerInitializerParam();
    for (int i = 0; i < 2; i++) {
      param.putLongParam("longParam"+i, new long[]{3141592, 123456});
      map.put("longFacet"+i, param);
    }

    return map;
  }
}
