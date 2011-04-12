package com.sensei.test;


import com.browseengine.bobo.api.BrowseSelection;
import com.browseengine.bobo.api.FacetSpec;
import com.browseengine.bobo.facets.DefaultFacetHandlerInitializerParam;
import com.browseengine.bobo.facets.FacetHandlerInitializerParam;
import com.sensei.search.client.servlet.DefaultSenseiJSONServlet;
import com.sensei.search.req.SenseiJSONQuery;
import com.sensei.search.req.SenseiQuery;
import com.sensei.search.req.SenseiRequest;
import com.sensei.search.svc.api.SenseiException;
import com.sensei.search.svc.impl.HttpRestSenseiServiceImpl;
import java.io.UnsupportedEncodingException;
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
import org.apache.lucene.search.SortField;
import org.json.JSONException;
import org.json.JSONObject;


public class TestHttpRestSenseiServiceImpl extends TestCase
{
  private final int EXPECTED_COUNT = 72;
  private final int EXPECTED_OFFSET = 227;
  private final boolean EXPECTED_FETCH_STORED_FIELDS = true;
  private final boolean EXPECTED_SHOW_EXPLANATION = true;

  public TestHttpRestSenseiServiceImpl(String name)
  {
    super(name);
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

    JSONObject obj = new JSONObject();

    obj.put("query", "key words");  // 'query' in the JSONObj gets translated into 'q' in the GET request

    for (int i = 0; i < 10; i++) {
      obj.put("key" + i, "val" + i);
    }

    SenseiQuery query = new SenseiJSONQuery(obj);

    return query;
  }

  SortField[] createSortFields() {
    List<SortField> list = new ArrayList<SortField>();

    list.add(new SortField(null, SortField.DOC));
    list.add(new SortField(null, SortField.SCORE));
    list.add(new SortField(null, SortField.SCORE, true));

//    list.add(new SortField("fieldDOC", SortField.DOC));
//    list.add(new SortField("fieldWOOT", SortField.SCORE));
    list.add(new SortField("fieldCUSTOM", SortField.CUSTOM, false));

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
      param.putLongParam("longParam"+i, new long[]{3141592});
      map.put("longFacet"+i, param);
    }

    return map;
  }
}
