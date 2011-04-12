package com.sensei.search.svc.impl;


import com.browseengine.bobo.api.BrowseFacet;
import com.browseengine.bobo.api.BrowseSelection;
import com.browseengine.bobo.api.FacetAccessible;
import com.browseengine.bobo.api.FacetIterator;
import com.browseengine.bobo.api.FacetSpec;
import com.browseengine.bobo.facets.FacetHandlerInitializerParam;
import com.sensei.search.client.servlet.SenseiSearchServletParams;
import com.sensei.search.req.SenseiHit;
import com.sensei.search.req.SenseiQuery;
import com.sensei.search.req.SenseiRequest;
import com.sensei.search.req.SenseiResult;
import com.sensei.search.req.SenseiSystemInfo;
import com.sensei.search.svc.api.SenseiException;
import com.sensei.search.svc.api.SenseiService;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import javax.net.ssl.SSLHandshakeException;
import org.apache.http.HttpEntity;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.NoHttpResponseException;
import org.apache.http.client.HttpRequestRetryHandler;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIUtils;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.protocol.ExecutionContext;
import org.apache.http.protocol.HttpContext;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.SortField;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;


public class HttpRestSenseiServiceImpl implements SenseiService
{
  String _scheme;
  String _host;
  int _port;
  String _path;
  int _maxRetries;
  HttpRequestRetryHandler _retryHandler;

  public HttpRestSenseiServiceImpl(
      String scheme,
      String host,
      int port,
      String path,
      final int maxRetries)
  {
    this(scheme,
         host,
         port,
         path,
         maxRetries,
         new HttpRequestRetryHandler()
         {
           public boolean retryRequest(IOException exception, int executionCount, HttpContext context)
           {
             if (executionCount >= maxRetries)
             {
               // Do not retry if over max retry count
               return false;
             }
             if (exception instanceof NoHttpResponseException)
             {
               // Retry if the server dropped connection on us
               return true;
             }
             if (exception instanceof SSLHandshakeException)
             {
               // Do not retry on SSL handshake exception
               return false;
             }
             HttpRequest request = (HttpRequest) context.getAttribute(ExecutionContext.HTTP_REQUEST);
             boolean idempotent = !(request instanceof HttpEntityEnclosingRequest);
             if (idempotent)
             {
               // Retry if the request is considered idempotent
               return true;
             }
             return false;
           }
         });
  }

  public HttpRestSenseiServiceImpl(String scheme,
                                   String host,
                                   int port,
                                   String path,
                                   final int maxRetries,
                                   HttpRequestRetryHandler retryHandler)
  {
    _scheme = scheme;
    _host = host;
    _port = port;
    _path = path;
    _maxRetries = maxRetries;
    _retryHandler = retryHandler;
  }

  @Override
  public SenseiResult doQuery(SenseiRequest req)
      throws SenseiException
  {
    SenseiResult result;

    try
    {
      List<NameValuePair> queryParams = convertRequestToQueryParams(req);
      URI requestURI = buildRequestURI(queryParams);
      InputStream is = makeRequest(requestURI);
      JSONObject jsonObj = convertStreamToJSONObject(is);
      result = buildSenseiResult(jsonObj);
    }
    catch (URISyntaxException e)
    {
      throw new SenseiException(e);
    }
    catch (IOException e)
    {
      throw new SenseiException(e);
    }
    catch (JSONException e)
    {
      throw new SenseiException(e);
    }

    return result;
  }

  @Override
  public SenseiSystemInfo getSystemInfo()
      throws SenseiException
  {
    throw new NotImplementedException();
  }

  public static List<NameValuePair> convertRequestToQueryParams(SenseiRequest req)
      throws SenseiException, UnsupportedEncodingException
  {
    List<NameValuePair> qparams = new ArrayList<NameValuePair>();

    convertScalarParams(qparams, req);
    convertSortFieldParams(qparams, req.getSort());
    convertSenseiQuery(qparams, req.getQuery());
    convertSelectionNames(qparams, req);
    convertFacetSpecs(qparams, req.getFacetSpecs());
    convertFacetInitParams(qparams, req.getFacetHandlerInitParamMap());
    convertPartitionParams(qparams, req.getPartitions());

    return qparams;
  }

  public static void convertSortFieldParams(List<NameValuePair> qparams, SortField[] sortFields) {
    List<String> fieldList = new ArrayList<String>();

    for (SortField field : sortFields) {
      fieldList.add(convertSortField(field));
    }

    String paramList = join(fieldList, ",");

    qparams.add(new BasicNameValuePair(SenseiSearchServletParams.PARAM_SORT, paramList));
  }

  public static void convertPartitionParams(List<NameValuePair> qparams, Set<Integer> partitions) {
    if (partitions == null || partitions.size() == 0) return;

    qparams.add(new BasicNameValuePair(SenseiSearchServletParams.PARAM_PARTITIONS, join(partitions, ",")));
  }

  public static void convertScalarParams(List<NameValuePair> qparams, SenseiRequest req) {
    qparams.add(new BasicNameValuePair(SenseiSearchServletParams.PARAM_FETCH_STORED, Boolean.toString(req.isFetchStoredFields())));
    qparams.add(new BasicNameValuePair(SenseiSearchServletParams.PARAM_SHOW_EXPLAIN, Boolean.toString(req.isShowExplanation())));
    qparams.add(new BasicNameValuePair(SenseiSearchServletParams.PARAM_OFFSET, Integer.toString(req.getOffset())));
    qparams.add(new BasicNameValuePair(SenseiSearchServletParams.PARAM_COUNT, Integer.toString(req.getCount())));
  }

  public static void convertFacetInitParams(List<NameValuePair> qparams, Map<String,FacetHandlerInitializerParam> initParams)
      throws UnsupportedEncodingException
  {
    final String format = "%s.%s.%s.%s";

    for (String facetName : initParams.keySet()) {

      FacetHandlerInitializerParam param = initParams.get(facetName);

      for (String paramName : param.getBooleanParamNames()) {
        qparams.add(new BasicNameValuePair(
            String.format(format, SenseiSearchServletParams.PARAM_DYNAMIC_INIT, facetName, paramName, SenseiSearchServletParams.PARAM_DYNAMIC_TYPE ),
            SenseiSearchServletParams.PARAM_DYNAMIC_TYPE_BOOL));
        qparams.add(new BasicNameValuePair(
            String.format(format, SenseiSearchServletParams.PARAM_DYNAMIC_INIT, facetName, paramName, SenseiSearchServletParams.PARAM_DYNAMIC_VAL),
            Boolean.toString(param.getBooleanParam(paramName)[0])));
      }

      for (String paramName : param.getByteArrayParamNames()) {
        qparams.add(new BasicNameValuePair(
            String.format(format, SenseiSearchServletParams.PARAM_DYNAMIC_INIT, facetName, paramName, SenseiSearchServletParams.PARAM_DYNAMIC_TYPE),
            SenseiSearchServletParams.PARAM_DYNAMIC_TYPE_BYTEARRAY));
        qparams.add(new BasicNameValuePair(
            String.format(format, SenseiSearchServletParams.PARAM_DYNAMIC_INIT, facetName, paramName, SenseiSearchServletParams.PARAM_DYNAMIC_VAL),
            new String(param.getByteArrayParam(paramName), "UTF-8")));
      }

      for (String paramName : param.getDoubleParamNames()) {
        qparams.add(new BasicNameValuePair(
            String.format(format, SenseiSearchServletParams.PARAM_DYNAMIC_INIT, facetName, paramName, SenseiSearchServletParams.PARAM_DYNAMIC_TYPE),
            SenseiSearchServletParams.PARAM_DYNAMIC_TYPE_DOUBLE));
        qparams.add(new BasicNameValuePair(
            String.format(format, SenseiSearchServletParams.PARAM_DYNAMIC_INIT, facetName, paramName, SenseiSearchServletParams.PARAM_DYNAMIC_VAL),
            Double.toString(param.getDoubleParam(paramName)[0])));
      }

      for (String paramName : param.getIntParamNames()) {
        qparams.add(new BasicNameValuePair(
            String.format(format, SenseiSearchServletParams.PARAM_DYNAMIC_INIT, facetName, paramName, SenseiSearchServletParams.PARAM_DYNAMIC_TYPE),
            SenseiSearchServletParams.PARAM_DYNAMIC_TYPE_INT));
        qparams.add(new BasicNameValuePair(
            String.format(format, SenseiSearchServletParams.PARAM_DYNAMIC_INIT, facetName, paramName, SenseiSearchServletParams.PARAM_DYNAMIC_VAL),
            Integer.toString(param.getIntParam(paramName)[0])));
      }

      for (String paramName : param.getLongParamNames()) {
        qparams.add(new BasicNameValuePair(
            String.format(format, SenseiSearchServletParams.PARAM_DYNAMIC_INIT, facetName, paramName, SenseiSearchServletParams.PARAM_DYNAMIC_TYPE),
            SenseiSearchServletParams.PARAM_DYNAMIC_TYPE_LONG));
        qparams.add(new BasicNameValuePair(
            String.format(format, SenseiSearchServletParams.PARAM_DYNAMIC_INIT, facetName, paramName, SenseiSearchServletParams.PARAM_DYNAMIC_VAL),
            Long.toString(param.getLongParam(paramName)[0])));
      }

      for (String paramName : param.getStringParamNames()) {
        qparams.add(new BasicNameValuePair(
            String.format(format, SenseiSearchServletParams.PARAM_DYNAMIC_INIT, facetName, paramName, SenseiSearchServletParams.PARAM_DYNAMIC_TYPE),
            SenseiSearchServletParams.PARAM_DYNAMIC_TYPE_STRING));
        qparams.add(new BasicNameValuePair(
            String.format(format, SenseiSearchServletParams.PARAM_DYNAMIC_INIT, facetName, paramName, SenseiSearchServletParams.PARAM_DYNAMIC_VAL),
            param.getStringParam(paramName).get(0)));
      }
    }
  }

  public static void convertFacetSpecs(List<NameValuePair> qparams, Map<String,FacetSpec> facetSpecs) {
    final String format = "%s.%s.%s";

    for (String facetName : facetSpecs.keySet()) {

      FacetSpec spec = facetSpecs.get(facetName);

      qparams.add(new BasicNameValuePair(
          String.format(format, SenseiSearchServletParams.PARAM_FACET, facetName, SenseiSearchServletParams.PARAM_FACET_MAX),
          Integer.toString(spec.getMaxCount())));
      qparams.add(new BasicNameValuePair(
          String.format(format, SenseiSearchServletParams.PARAM_FACET, facetName, SenseiSearchServletParams.PARAM_FACET_ORDER),
          convertFacetSortSpec(spec.getOrderBy())));
      qparams.add(new BasicNameValuePair(
          String.format(format, SenseiSearchServletParams.PARAM_FACET, facetName, SenseiSearchServletParams.PARAM_FACET_EXPAND),
          Boolean.toString(spec.isExpandSelection())));
      qparams.add(new BasicNameValuePair(
          String.format(format, SenseiSearchServletParams.PARAM_FACET, facetName, SenseiSearchServletParams.PARAM_FACET_MINHIT),
          Integer.toString(spec.getMinHitCount())));
    }
  }

  public static String convertFacetSortSpec(FacetSpec.FacetSortSpec spec) {
    switch (spec)
    {
      case OrderValueAsc:
        return SenseiSearchServletParams.PARAM_FACET_ORDER_VAL;
      case OrderHitsDesc:
        return SenseiSearchServletParams.PARAM_FACET_ORDER_HITS;
      case OrderByCustom:
      default:
        throw new IllegalArgumentException("invalid order string: " + spec);
    }
  }



  public static String convertSortField(SortField field) {
    String result;

    if (field.equals(SenseiRequest.FIELD_SCORE)) {
      result = SenseiSearchServletParams.PARAM_SORT_SCORE;
    } else if (field.equals(SenseiRequest.FIELD_SCORE_REVERSE)) {
      result = SenseiSearchServletParams.PARAM_SORT_SCORE_REVERSE;
    } else if (field.equals(SenseiRequest.FIELD_DOC)) {
      result = SenseiSearchServletParams.PARAM_SORT_DOC;
    } else if (field.equals(SenseiRequest.FIELD_DOC_REVERSE)) {
      result = SenseiSearchServletParams.PARAM_SORT_DOC_REVERSE;
    } else {
      result = String.format(
          "%s:%s",
          field.getField(),
          field.getReverse()
              ? SenseiSearchServletParams.PARAM_SORT_DESC
              : SenseiSearchServletParams.PARAM_SORT_ASC);
    }

    return result;
  }

  public static void convertSelectionNames(List<NameValuePair> qparams, SenseiRequest req) {
    Set<String> selectionNames = req.getSelectionNames();

    final String format = "%s.%s.%s";

    for (String selectionName : selectionNames) {
      BrowseSelection selection = req.getSelection(selectionName);

      qparams.add(new BasicNameValuePair(
          String.format(format, SenseiSearchServletParams.PARAM_SELECT, selectionName, SenseiSearchServletParams.PARAM_SELECT_NOT),
          join(selection.getNotValues(), ",")));
      qparams.add(new BasicNameValuePair(
          String.format(format, SenseiSearchServletParams.PARAM_SELECT, selectionName, SenseiSearchServletParams.PARAM_SELECT_OP),
          convertSelectionOperation(selection.getSelectionOperation())));
      qparams.add(new BasicNameValuePair(
          String.format(format, SenseiSearchServletParams.PARAM_SELECT, selectionName, SenseiSearchServletParams.PARAM_SELECT_VAL),
          join(selection.getValues(), ",")));
      if (selection.getSelectionProperties().size() > 0) {
        qparams.add(new BasicNameValuePair(
            String.format(format, SenseiSearchServletParams.PARAM_SELECT, selectionName, SenseiSearchServletParams.PARAM_SELECT_PROP),
            convertSelectionProperties(selection.getSelectionProperties())));
      }
    }
  }

  private static String convertSelectionOperation(BrowseSelection.ValueOperation operation) {
    switch (operation)
    {
      case ValueOperationOr:
        return SenseiSearchServletParams.PARAM_SELECT_OP_OR;
      case ValueOperationAnd:
        return SenseiSearchServletParams.PARAM_SELECT_OP_AND;
      default:
        throw new IllegalArgumentException("unsupported selection operator");
    }
  }

  private static String convertSelectionProperties(Properties props) {
    List<String> propList = new ArrayList<String>(props.size());

    final String format = "%s:%s";

    for (Object keyObj : props.keySet()) {
      propList.add(String.format(format, keyObj, props.get(keyObj)));
    }

    return join(propList, ",");
  }

  public static void convertSenseiQuery(List<NameValuePair> qparams, SenseiQuery query)
      throws SenseiException
  {
    if (query == null) return;

    try
    {
      JSONObject jsonObj = new JSONObject(query.toString());
      Iterator iter = jsonObj.keys();

      final String format = "%s:%s";

      while (iter.hasNext()) {
        String key = (String) iter.next();

        if (key.equals("query")) {
          qparams.add(new BasicNameValuePair(SenseiSearchServletParams.PARAM_QUERY, (String) jsonObj.get(key)));
          continue;
        }

        qparams.add(new BasicNameValuePair(SenseiSearchServletParams.PARAM_QUERY_PARAM, String.format(format, key, jsonObj.get(key))));
      }
    }
    catch (JSONException e)
    {
      throw new SenseiException(e);
    }
  }

  public URI buildRequestURI(List<NameValuePair> qparams)
      throws URISyntaxException
  {
    URI uri =
      URIUtils.createURI(
        _scheme,
        _host,
        _port,
        _path,
        URLEncodedUtils.format(qparams, "UTF-8"),
        null);
    return uri;
  }

  public InputStream makeRequest(URI uri)
      throws IOException
  {
    DefaultHttpClient httpclient = new DefaultHttpClient();
    httpclient.setHttpRequestRetryHandler(_retryHandler);

    HttpGet httpget = new HttpGet(uri);
    HttpResponse response = httpclient.execute(httpget);
    HttpEntity entity = response.getEntity();
    if (entity == null)
    {
      throw new IOException("failed to complete request");
    }

    return entity.getContent();
  }

  public static String join(String[] arr, String delimiter) {
    return join(Arrays.asList(arr), delimiter);
  }

  public static String join(Collection<?> s, String delimiter) {
    StringBuilder builder = new StringBuilder();
    Iterator iter = s.iterator();
    while (iter.hasNext()) {
       builder.append(iter.next().toString());
       if (!iter.hasNext()) {
         break;
       }
       builder.append(delimiter);
    }
    return builder.toString();
  }

  public static String convertStreamToString(InputStream is)
      throws IOException
  {
    BufferedReader reader = new BufferedReader(new InputStreamReader(is));
    StringBuilder sb = new StringBuilder();

    try
    {
      String line;

      while ((line = reader.readLine()) != null)
      {
        sb.append(line + "\n");
      }
    }
    finally
    {
      is.close();
    }

    return sb.toString();
  }

  public static JSONObject convertStreamToJSONObject(InputStream is)
      throws IOException, JSONException
  {
    String rawJSON = convertStreamToString(is);
    return new JSONObject(rawJSON);
  }

  public static SenseiResult buildSenseiResult(JSONObject jsonObj)
      throws JSONException
  {
    SenseiResult result = new SenseiResult();

    result.setTotalDocs(jsonObj.getInt(SenseiSearchServletParams.PARAM_RESULT_TOTALDOCS));
    result.setParsedQuery(jsonObj.getString(SenseiSearchServletParams.PARAM_RESULT_PARSEDQUERY));
    result.setNumHits(jsonObj.getInt(SenseiSearchServletParams.PARAM_RESULT_NUMHITS));
    result.setParsedQuery(jsonObj.getString(SenseiSearchServletParams.PARAM_RESULT_PARSEDQUERY));
    result.setTime(jsonObj.getLong(SenseiSearchServletParams.PARAM_RESULT_TIME));
    result.addAll(convertFacetMap(jsonObj.getJSONObject(SenseiSearchServletParams.PARAM_RESULT_FACETS)));
    result.setHits(convertHitsArray(jsonObj.getJSONArray(SenseiSearchServletParams.PARAM_RESULT_HITS)));

    return result;
  }

  private static Map<String, FacetAccessible> convertFacetMap(JSONObject jsonObject)
      throws JSONException
  {
    Map<String, FacetAccessible> map = new HashMap <String, FacetAccessible>();

    Iterator iter = jsonObject.sortedKeys();

    while(iter.hasNext()) {
      String fieldName = (String) iter.next();
      JSONArray facetArr = (JSONArray)jsonObject.get(fieldName);
      int length = facetArr.length();

      BrowseFacet[] facets = new BrowseFacet[length];

      for (int i = 0; i < length; i++) {
        JSONObject facetObj = (JSONObject) facetArr.get(i);
        BrowseFacet bf = new BrowseFacet();
        bf.setFacetValueHitCount(facetObj.getInt("count"));
        bf.setValue(facetObj.getString("value"));
      }

      FacetAccessible fa = new MappedFacetAccessible(facets);

      map.put(fieldName, fa);
    }

    return map;
  }

  private static class MappedFacetAccessible implements FacetAccessible, Serializable
  {
    private static final long serialVersionUID = 1L;

    private final HashMap<String, BrowseFacet> _facetMap;
    private final BrowseFacet[] _facets;

    public MappedFacetAccessible(BrowseFacet[] facets)
    {
      _facetMap = new HashMap<String, BrowseFacet>();
      for (BrowseFacet facet : facets)
      {
	      if (facet != null ){
          _facetMap.put(facet.getValue(), facet);
        }
      }
      _facets = facets;
    }

    public BrowseFacet getFacet(String value)
    {
      return _facetMap.get(value);
    }

    public List<BrowseFacet> getFacets()
    {
      return Arrays.asList(_facets);
    }

    @Override
    public void close()
    {
    }

    @Override
    public FacetIterator iterator()
    {
      throw new IllegalStateException("FacetIterator should not be obtained at merge time");
    }

  }

  private static SenseiHit[] convertHitsArray(JSONArray hitsArray)
      throws JSONException
  {
    int hitsArrayLength = hitsArray.length();

    SenseiHit[] result = new SenseiHit[hitsArrayLength];

    for (int i = 0; i < hitsArrayLength; i++) {
      JSONObject hitObj = (JSONObject)hitsArray.get(i);

      SenseiHit hit = new SenseiHit();
      hit.setUID(hitObj.getLong(SenseiSearchServletParams.PARAM_RESULT_HIT_UID));
      hit.setScore((float) hitObj.getDouble(SenseiSearchServletParams.PARAM_RESULT_HIT_SCORE));
      hit.setExplanation(convertToExplanation(hitObj.getJSONObject(SenseiSearchServletParams.PARAM_RESULT_HIT_EXPLANATION)));

      result[i] = hit;
    }

    return result;
  }

  public static Explanation convertToExplanation(JSONObject jsonObj)
      throws JSONException
  {
    Explanation explanation = new Explanation();

    float value = (float) jsonObj.getDouble("value");
    String description = jsonObj.getString("description");

    explanation.setDescription(description);
    explanation.setValue(value);

    JSONArray detailsArr = jsonObj.getJSONArray("details");
    int detailsCnt = detailsArr.length();

    for (int i = 0; i < detailsCnt; i++) {
      JSONObject detailObj = (JSONObject) detailsArr.get(i);
      Explanation detailExpl = convertToExplanation(detailObj);
      explanation.addDetail(detailExpl);
    }

    return explanation;
  }

}
