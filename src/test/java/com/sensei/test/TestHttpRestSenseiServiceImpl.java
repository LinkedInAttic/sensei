package com.sensei.test;


import com.browseengine.bobo.api.BrowseSelection;
import com.browseengine.bobo.api.FacetSpec;
import com.browseengine.bobo.facets.DefaultFacetHandlerInitializerParam;
import com.browseengine.bobo.facets.FacetHandlerInitializerParam;
import com.sensei.search.client.servlet.DefaultSenseiJSONServlet;
import com.sensei.search.client.servlet.SenseiSearchServletParams;
import com.sensei.search.req.SenseiJSONQuery;
import com.sensei.search.req.SenseiQuery;
import com.sensei.search.req.SenseiRequest;
import com.sensei.search.svc.api.SenseiException;
import com.sensei.search.svc.impl.HttpRestSenseiServiceImpl;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
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

  public void testDoQuery()
  {
  }

  public void testGetSystemInfo()
  {

  }

  public void testConvertSenseiRequest()
      throws SenseiException, UnsupportedEncodingException, JSONException
  {
    SenseiRequest testRequest = createSenseiRequest();
    List<NameValuePair> list = HttpRestSenseiServiceImpl.convertRequestToQueryParams(testRequest);
    MockServletRequest mockServletRequest = MockServletRequest.create(list);
    DataConfiguration params = new DataConfiguration(new ServletRequestConfiguration(mockServletRequest));
    SenseiRequest resultRequest = DefaultSenseiJSONServlet.convertSenseiRequest(params);
    assertEquals(testRequest, resultRequest);
  }

  private void assertEquals(SenseiRequest a, SenseiRequest b) {
    assertEquals(a.getCount(), b.getCount());
    assertEquals(a.getOffset(), b.getOffset());
    assertFacetSpecEquals(a.getFacetSpecs(), a.getFacetSpecs());
    assertEquals(a.getSelections(), b.getSelections());
    assertInitParamsEquals(a.getFacetHandlerInitParamMap(), b.getFacetHandlerInitParamMap());
    assertEquals(a.getQuery(), b.getQuery());
    assertEquals(a.getSort(), b.getSort());
//    assertEquals(a.getPartitions(), b.getPartitions());
//    assertEquals(a.getTid(), b.getTid());
  }

  private void assertEquals(SortField[] a, SortField[] b) {
    assertTrue(Arrays.equals(a, b));
  }

  private void assertEquals(SenseiQuery a, SenseiQuery b) {
    assertEquals(a.toString(), b.toString());
  }

  private void assertInitParamsEquals(Map<String,FacetHandlerInitializerParam> a, Map<String,FacetHandlerInitializerParam> b) {
    assertEquals(a.size(), b.size());

    for (String key : a.keySet()) {
      assertTrue(b.containsKey(key));
      assertEquals(a.get(key), b.get(key));
    }
  }

  private void assertEquals(FacetHandlerInitializerParam a, FacetHandlerInitializerParam b) {
    assertEquals(a.getBooleanParamNames(), b.getBooleanParamNames());
    assertEquals(a.getIntParamNames(), b.getIntParamNames());
    assertEquals(a.getDoubleParamNames(), b.getDoubleParamNames());
    assertEquals(a.getLongParamNames(), b.getLongParamNames());
    assertEquals(a.getStringParamNames(), b.getStringParamNames());
    assertEquals(a.getByteArrayParamNames(), b.getByteArrayParamNames());

    for (String name : a.getBooleanParamNames()) {
      assertTrue(Arrays.equals(a.getBooleanParam(name), b.getBooleanParam(name)));
    }
    for (String name : a.getIntParamNames()) {
      assertTrue(Arrays.equals(a.getIntParam(name), b.getIntParam(name)));
    }
    for (String name : a.getDoubleParamNames()) {
      assertTrue(Arrays.equals(a.getDoubleParam(name), b.getDoubleParam(name)));
    }
    for (String name : a.getLongParamNames()) {
      assertTrue(Arrays.equals(a.getLongParam(name), b.getLongParam(name)));
    }
    for (String name : a.getStringParamNames()) {
      assertTrue(Arrays.equals(a.getStringParam(name).toArray(new String[0]), b.getStringParam(name).toArray(new String[0])));
    }
/* NOT YET SUPPORTED
    for (String name : a.getByteArrayParamNames()) {
      assertTrue(Arrays.equals(a.getByteArrayParam(name), b.getByteArrayParam(name)));
    }
*/
  }

  private void assertEquals(BrowseSelection[] a, BrowseSelection[] b) {
    assertEquals(a.length, b.length);

    for (int i = 0; i < a.length; i++) {
      assertEquals(a[i], b[i]);
    }
  }

  private void assertEquals(BrowseSelection a, BrowseSelection b) {
    assertEquals(a.getFieldName(), b.getFieldName());
    assertTrue(Arrays.equals(a.getValues(), b.getValues()));
    assertTrue(Arrays.equals(a.getNotValues(), b.getNotValues()));
    assertEquals(a.getSelectionOperation(), b.getSelectionOperation());
    // TODO: verify this actually does the correct equals
    assertEquals(a.getSelectionProperties(), b.getSelectionProperties());
  }

  private void assertFacetSpecEquals(Map<String,FacetSpec> a, Map<String,FacetSpec> b) {
    assertEquals(a.size(), b.size());

    for (String key : a.keySet()) {
      assertTrue(b.containsKey(key));
      assertEquals(a.get(key), b.get(key));
    }
  }

  private <T> void assertEquals(Set<T> a, Set<T> b) {
    assertEquals(a.size(), b.size());

    Iterator iter = a.iterator();
    while (iter.hasNext()) {
      T val = (T)iter.next();
      assertTrue(b.contains(val));
    }
  }

  private void assertEquals(FacetSpec a, FacetSpec b) {
    assertEquals(a.getMaxCount(), b.getMaxCount());
    assertEquals(a.getMinHitCount(), b.getMinHitCount());
    assertEquals(a.getOrderBy(), b.getOrderBy());
    assertEquals(a.isExpandSelection(), b.isExpandSelection());
  }

  public void testConvertSenseiRequestValues()
      throws SenseiException, UnsupportedEncodingException, JSONException
  {
    SenseiRequest req = createSenseiRequest();
    List<NameValuePair> list = HttpRestSenseiServiceImpl.convertRequestToQueryParams(req);

    for (NameValuePair pair : list) {
      if (pair.getName() == SenseiSearchServletParams.PARAM_FETCH_STORED) {
        assertEquals(pair.getValue(), Boolean.toString(req.isFetchStoredFields()));
        continue;
      }
      if (pair.getName() == SenseiSearchServletParams.PARAM_SHOW_EXPLAIN) {
        assertEquals(pair.getValue(), Boolean.toString(req.isShowExplanation()));
        continue;
      }
      if (pair.getName() == SenseiSearchServletParams.PARAM_OFFSET) {
        assertEquals(pair.getValue(), Integer.toString(req.getOffset()));
        continue;
      }
      if (pair.getName() == SenseiSearchServletParams.PARAM_COUNT) {
        assertEquals(pair.getValue(), Integer.toString(req.getCount()));
        continue;
      }
      if (pair.getName() == SenseiSearchServletParams.PARAM_SORT) {
        assertEquals(pair.getValue(), HttpRestSenseiServiceImpl.convertSortFields(req.getSort()));
        continue;
      }
    }
  }

  public void testInitParams()
      throws UnsupportedEncodingException
  {
    List<NameValuePair> list = HttpRestSenseiServiceImpl.convertFacetInitParams(createInitParams());
  }

  public void testFacetSpecs()
  {
    List<NameValuePair> list = HttpRestSenseiServiceImpl.convertFacetSpecs(createFacetSpecMap());
  }

  public void testSortFields()
  {
    String list = HttpRestSenseiServiceImpl.convertSortFields(createSortFields());
  }

  public void testQuery()
      throws SenseiException, JSONException
  {
    List<NameValuePair> list = HttpRestSenseiServiceImpl.convertSenseiQuery(createSenseiQuery());
  }

  public void testSelections()
      throws JSONException
  {
    SenseiRequest req = createSenseiRequest();
    List<NameValuePair> list = HttpRestSenseiServiceImpl.convertSelectionNames(req);
  }

  private SenseiRequest createSenseiRequest()
      throws JSONException
  {
    SenseiRequest req = new SenseiRequest();

    req.setCount(EXPECTED_COUNT);
    req.setOffset(EXPECTED_OFFSET);
    req.setFetchStoredFields(EXPECTED_FETCH_STORED_FIELDS);
    req.setShowExplanation(EXPECTED_SHOW_EXPLANATION);

    req.setFacetHandlerInitParamMap(createInitParams());
    req.setFacetSpecs(createFacetSpecMap());
    req.setSort(createSortFields());
    req.setQuery(createSenseiQuery());
    req.addSelections(createBrowseSelections());

    return req;
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
//      obj.put(SenseiSearchServletParams.PARAM_QUERY_PARAM, String.format("%s:%s", "key" + i, "val" + i));
      obj.put("key" + i, "val" + i);
    }

    SenseiQuery query = new SenseiJSONQuery(obj);

    return query;
  }

  SortField[] createSortFields() {
    List<SortField> list = new ArrayList<SortField>();

    return list.toArray(new SortField[list.size()]);
  }

  Map<String, FacetSpec> createFacetSpecMap()
  {
    Map<String, FacetSpec> map = new HashMap<String, FacetSpec>();

    return map;
  }

  Map<String, FacetHandlerInitializerParam> createInitParams()
  {
    Map<String, FacetHandlerInitializerParam> map = new HashMap<String, FacetHandlerInitializerParam>();

    DefaultFacetHandlerInitializerParam param;

    param = new DefaultFacetHandlerInitializerParam();
    param.putBooleanParam("boolParam", new boolean[]{false});
    map.put("boolFacet", param);

    param = new DefaultFacetHandlerInitializerParam();
    param.putIntParam("intParam", new int[]{42});
    map.put("intFacet", param);

/*  NOT YET SUPPORTED
    param = new DefaultFacetHandlerInitializerParam();
    param.putByteArrayParam("bytearrayParam", new byte[]{1, 2, 3});
    map.put("bytearrayFacet", param);
 */

    param = new DefaultFacetHandlerInitializerParam();
    param.putStringParam("stringParam", new ArrayList<String>(){{ add("woot"); }});
    map.put("stringFacet", param);

    param = new DefaultFacetHandlerInitializerParam();
    param.putDoubleParam("doubleParam", new double[]{3.141592});
    map.put("doubleFacet", param);

    param = new DefaultFacetHandlerInitializerParam();
    param.putLongParam("longParam", new long[]{3141592});
    map.put("longFacet", param);

    return map;
  }
}
