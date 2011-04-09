package com.sensei.search.svc.impl;


import com.browseengine.bobo.api.BrowseSelection;
import com.browseengine.bobo.api.FacetSpec;
import com.browseengine.bobo.facets.FacetHandlerInitializerParam;
import com.sensei.search.client.servlet.SenseiSearchServletParams;
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
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
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
import org.apache.lucene.search.SortField;
import org.json.JSONException;
import org.json.JSONObject;


public class HttpRestSenseiServiceImpl implements SenseiService
{
  String _scheme;
  String _host;
  int _port;
  String _path;
  int _maxRetries;
  HttpRequestRetryHandler _retryHandler;

  public HttpRestSenseiServiceImpl(String scheme, String host, int port, String path, final int maxRetries)
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
                                   int maxRetries,
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
    // TODO Auto-generated method stub
    return null;
  }

  public static List<NameValuePair> convertRequestToQueryParams(SenseiRequest req)
      throws SenseiException, UnsupportedEncodingException
  {
    List<NameValuePair> qparams = new ArrayList<NameValuePair>();

    qparams.add(new BasicNameValuePair(SenseiSearchServletParams.PARAM_FETCH_STORED, Boolean.toString(req.isFetchStoredFields())));
    qparams.add(new BasicNameValuePair(SenseiSearchServletParams.PARAM_SHOW_EXPLAIN, Boolean.toString(req.isShowExplanation())));
    qparams.add(new BasicNameValuePair(SenseiSearchServletParams.PARAM_OFFSET, Integer.toString(req.getOffset())));
    qparams.add(new BasicNameValuePair(SenseiSearchServletParams.PARAM_COUNT, Integer.toString(req.getCount())));
    qparams.add(new BasicNameValuePair(SenseiSearchServletParams.PARAM_SORT, convertSortFields(req.getSort())));

    qparams.addAll(convertSenseiQuery(req.getQuery()));
    qparams.addAll(convertSelectionNames(req));
    qparams.addAll(convertFacetSpecs(req.getFacetSpecs()));
    qparams.addAll(convertFacetInitParams(req.getFacetHandlerInitParamMap()));

//    Set<Integer> partitions = req.getPartitions();

    return qparams;
  }

  public static List<NameValuePair> convertFacetInitParams(Map<String,FacetHandlerInitializerParam> initParams)
      throws UnsupportedEncodingException
  {
    List<NameValuePair> qparams = new ArrayList<NameValuePair>();

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

    return qparams;
  }

  public static List<NameValuePair> convertFacetSpecs(Map<String,FacetSpec> facetSpecs) {
    List<NameValuePair> qparams = new ArrayList<NameValuePair>();

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

    return qparams;
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

  public static String convertSortFields(SortField[] sortFields) {
    List<String> fieldList = new ArrayList<String>();

    for (SortField field : sortFields) {
      fieldList.add(convertSortField(field));
    }

    return join(fieldList, ",");
  }

  public static String convertSortField(SortField field) {
    String result;

    if (field == SortField.FIELD_SCORE) {
      result = SenseiSearchServletParams.PARAM_SORT_SCORE;
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

  public static List<NameValuePair> convertSelectionNames(SenseiRequest req) {
    List<NameValuePair> qparams = new ArrayList<NameValuePair>();

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

    return qparams;
  }

  public static String convertSelectionOperation(BrowseSelection.ValueOperation operation) {
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

  public static String convertSelectionProperties(Properties props) {
    List<String> propList = new ArrayList<String>(props.size());

    final String format = "%s:%s";

    for (Object keyObj : props.keySet()) {
      propList.add(String.format(format, keyObj, props.get(keyObj)));
    }

    return join(propList, ",");
  }

  public static List<NameValuePair> convertSenseiQuery(SenseiQuery query)
      throws SenseiException
  {
    List<NameValuePair> qparams = new ArrayList<NameValuePair>();

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

        qparams.add(new BasicNameValuePair(SenseiSearchServletParams.PARAM_QUERY_PARAM, String.format(format, key, (String) jsonObj.get(key))));
      }
    }
    catch (JSONException e)
    {
      throw new SenseiException(e);
    }

    return qparams;
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
       builder.append(iter.next());
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
  {
    SenseiResult result = new SenseiResult();
    return result;
  }
}
