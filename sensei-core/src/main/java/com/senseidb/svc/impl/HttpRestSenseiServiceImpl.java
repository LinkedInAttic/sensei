package com.senseidb.svc.impl;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.zip.GZIPInputStream;

import javax.net.ssl.SSLHandshakeException;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.http.Header;
import org.apache.http.HeaderElement;
import org.apache.http.HeaderElementIterator;
import org.apache.http.HttpEntity;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.HttpResponse;
import org.apache.http.HttpResponseInterceptor;
import org.apache.http.NameValuePair;
import org.apache.http.NoHttpResponseException;
import org.apache.http.client.HttpRequestRetryHandler;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIUtils;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.conn.scheme.PlainSocketFactory;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.entity.HttpEntityWrapper;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.tsccm.ThreadSafeClientConnManager;
import org.apache.http.message.BasicHeaderElementIterator;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpParams;
import org.apache.http.protocol.ExecutionContext;
import org.apache.http.protocol.HTTP;
import org.apache.http.protocol.HttpContext;
import org.apache.log4j.Logger;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.SortField;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.browseengine.bobo.api.BrowseFacet;
import com.browseengine.bobo.api.BrowseSelection;
import com.browseengine.bobo.api.FacetAccessible;
import com.browseengine.bobo.api.FacetSpec;
import com.browseengine.bobo.api.MappedFacetAccessible;
import com.browseengine.bobo.facets.FacetHandlerInitializerParam;
import com.senseidb.search.req.SenseiHit;
import com.senseidb.search.req.SenseiQuery;
import com.senseidb.search.req.SenseiRequest;
import com.senseidb.search.req.SenseiResult;
import com.senseidb.search.req.SenseiSystemInfo;
import com.senseidb.servlet.SenseiSearchServletParams;
import com.senseidb.svc.api.SenseiException;
import com.senseidb.svc.api.SenseiService;
import com.senseidb.util.JSONUtil.FastJSONArray;
import com.senseidb.util.JSONUtil.FastJSONObject;


public class HttpRestSenseiServiceImpl implements SenseiService
{
  private static final Logger log = Logger.getLogger(HttpRestSenseiServiceImpl.class);
  String _scheme;
  String _host;
  int _port;
  String _path;
  int _defaultKeepAliveDurationMS;
  int _maxRetries;
  DefaultHttpClient _httpclient;

  public HttpRestSenseiServiceImpl(
      String scheme,
      String host,
      int port,
      String path)
  {
    this(
      scheme,
      host,
      port,
      path,
      5000,
      5);
  }

  public HttpRestSenseiServiceImpl(
      String scheme,
      String host,
      int port,
      String path,
      int defaultKeepAliveDurationMS,
      final int maxRetries)
  {
    this(scheme,
         host,
         port,
         path,
         defaultKeepAliveDurationMS,
         maxRetries,
         null);
  }

  public HttpRestSenseiServiceImpl(String scheme,
                                   String host,
                                   int port,
                                   String path,
                                   int defaultKeepAliveDurationMS,
                                   final int maxRetries,
                                   HttpRequestRetryHandler retryHandler)
  {
    _scheme = scheme;
    _host = host;
    _port = port;
    _path = path;
    _defaultKeepAliveDurationMS = defaultKeepAliveDurationMS;
    _maxRetries = maxRetries;
    _httpclient = createHttpClient(retryHandler);
  }
  
  public HttpRestSenseiServiceImpl(String urlString) throws MalformedURLException{
	URL url = new URL(urlString);
	_scheme = url.getProtocol();
	_host = url.getHost();
	_port = url.getPort();
	_path = url.getPath();
	_defaultKeepAliveDurationMS = 5000;
	_maxRetries = 5;
	_httpclient = createHttpClient(null);
  }

  private DefaultHttpClient createHttpClient(HttpRequestRetryHandler retryHandler)
  {
    HttpParams params = new BasicHttpParams();
    SchemeRegistry registry = new SchemeRegistry();
    registry.register(new Scheme(_scheme, _port, PlainSocketFactory.getSocketFactory()));
    ClientConnectionManager cm = new ThreadSafeClientConnManager(registry);
    DefaultHttpClient client = new DefaultHttpClient(cm, params);
    if (retryHandler == null)
    {
      retryHandler = new HttpRequestRetryHandler()
      {
        public boolean retryRequest(IOException exception, int executionCount, HttpContext context)
        {
          if (executionCount >= _maxRetries)
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
      };
    }
    client.setHttpRequestRetryHandler(retryHandler);

    client.addRequestInterceptor(new HttpRequestInterceptor()
    {
      public void process(final HttpRequest request, final HttpContext context)
        throws HttpException, IOException
      {
        if (!request.containsHeader("Accept-Encoding"))
        {
          request.addHeader("Accept-Encoding", "gzip");
        }
      }
    });

    client.addResponseInterceptor(new HttpResponseInterceptor()
    {
      public void process(final HttpResponse response, final HttpContext context)
        throws HttpException, IOException
      {
        HttpEntity entity = response.getEntity();
        Header ceheader = entity.getContentEncoding();
        if (ceheader != null)
        {
          HeaderElement[] codecs = ceheader.getElements();
          for (int i = 0; i < codecs.length; i++)
          {
            if (codecs[i].getName().equalsIgnoreCase("gzip"))
            {
              response.setEntity(new GzipDecompressingEntity(response.getEntity()));
              return;
            }
          }
        }
      }
    });

    client.setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy()
    {
      @Override
      public long getKeepAliveDuration(HttpResponse response, HttpContext context)
      {
        // Honor 'keep-alive' header
        HeaderElementIterator it = new BasicHeaderElementIterator(response.headerIterator(HTTP.CONN_KEEP_ALIVE));
        while (it.hasNext())
        {
          HeaderElement he = it.nextElement();
          String param = he.getName();
          String value = he.getValue();
          if ((value != null) && param.equalsIgnoreCase("timeout"))
          {
            try
            {
              return Long.parseLong(value) * 1000;
            }
            catch (NumberFormatException ignore)
            {
            }
          }
        }

        long keepAlive = super.getKeepAliveDuration(response, context);
        if (keepAlive == -1)
        {
          keepAlive = _defaultKeepAliveDurationMS;
        }
        return keepAlive;
      }
    });

    return client;
  }

  private static class GzipDecompressingEntity extends HttpEntityWrapper
  {
    public GzipDecompressingEntity(final HttpEntity entity)
    {
      super(entity);
    }

    @Override
    public InputStream getContent()
      throws IOException, IllegalStateException
    {
      // the wrapped entity's getContent() decides about repeatability
      InputStream wrappedin = wrappedEntity.getContent();
      return new GZIPInputStream(wrappedin);
    }

    @Override
    public long getContentLength()
    {
      // length of ungzipped content is not known
      return -1;
    }

  }

  @Override
  public SenseiResult doQuery(SenseiRequest req)
      throws SenseiException
  {
    SenseiResult result;
    InputStream is = null;

    try
    {
      List<NameValuePair> queryParams = convertRequestToQueryParams(req);
      URI requestURI = buildRequestURI(queryParams);
      is = makeRequest(requestURI);
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
    finally
    {
      if (is != null)
      {
    	  IOUtils.closeQuietly(is);
      }
    }

    return result;
  }

  @Override
  public SenseiSystemInfo getSystemInfo()
      throws SenseiException
  {
    SenseiSystemInfo result;
    InputStream is = null;

    try
    {
      URI requestURI = buildSysInfoRequestURI();
      is = makeRequest(requestURI);
      JSONObject jsonObj = convertStreamToJSONObject(is);
      result = buildSysInfo(jsonObj);
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
    finally
    {
      if (is != null)
      {
    	  IOUtils.closeQuietly(is);
      }
    }

    return result;
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
    qparams.add(new BasicNameValuePair(SenseiSearchServletParams.PARAM_FETCH_STORED_VALUE, Boolean.toString(req.isFetchStoredValue())));
    qparams.add(new BasicNameValuePair(SenseiSearchServletParams.PARAM_SHOW_EXPLAIN, Boolean.toString(req.isShowExplanation())));
    qparams.add(new BasicNameValuePair(SenseiSearchServletParams.PARAM_OFFSET, Integer.toString(req.getOffset())));
    qparams.add(new BasicNameValuePair(SenseiSearchServletParams.PARAM_COUNT, Integer.toString(req.getCount())));
    Set<String> tvFetch = req.getTermVectorsToFetch();
    if (tvFetch!=null && tvFetch.size()>0){
      String fetchString = join(tvFetch,",");
      qparams.add(new BasicNameValuePair(SenseiSearchServletParams.PARAM_FETCH_TERMVECTOR, fetchString));
    }
    if (req.getRouteParam() != null)
    {
      qparams.add(new BasicNameValuePair(SenseiSearchServletParams.PARAM_ROUTE_PARAM, req.getRouteParam()));
    }

    if (req.getGroupBy() != null)
    {
      qparams.add(new BasicNameValuePair(SenseiSearchServletParams.PARAM_GROUP_BY, StringUtils.join(req.getGroupBy(), ',')));
    }

    if (req.getMaxPerGroup() > 0)
    {
      qparams.add(new BasicNameValuePair(SenseiSearchServletParams.PARAM_MAX_PER_GROUP, Integer.toString(req.getMaxPerGroup())));
    }

  }

  public static void convertFacetInitParams(List<NameValuePair> qparams, Map<String,FacetHandlerInitializerParam> initParams)
      throws UnsupportedEncodingException
  {
    final String format = "%s.%s.%s.%s";

    for (Entry<String,FacetHandlerInitializerParam> entry : initParams.entrySet()) {
      String facetName = entry.getKey();
      FacetHandlerInitializerParam param = entry.getValue();

      for (String paramName : param.getBooleanParamNames()) {
        qparams.add(new BasicNameValuePair(
            String.format(format, SenseiSearchServletParams.PARAM_DYNAMIC_INIT, facetName, paramName, SenseiSearchServletParams.PARAM_DYNAMIC_TYPE ),
            SenseiSearchServletParams.PARAM_DYNAMIC_TYPE_BOOL));
        qparams.add(new BasicNameValuePair(
            String.format(format, SenseiSearchServletParams.PARAM_DYNAMIC_INIT, facetName, paramName, SenseiSearchServletParams.PARAM_DYNAMIC_VAL),
            join(param.getBooleanParam(paramName), ",")));
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
            join(param.getDoubleParam(paramName), ",")));
      }

      for (String paramName : param.getIntParamNames()) {
        qparams.add(new BasicNameValuePair(
            String.format(format, SenseiSearchServletParams.PARAM_DYNAMIC_INIT, facetName, paramName, SenseiSearchServletParams.PARAM_DYNAMIC_TYPE),
            SenseiSearchServletParams.PARAM_DYNAMIC_TYPE_INT));
        qparams.add(new BasicNameValuePair(
            String.format(format, SenseiSearchServletParams.PARAM_DYNAMIC_INIT, facetName, paramName, SenseiSearchServletParams.PARAM_DYNAMIC_VAL),
            join(param.getIntParam(paramName), ",")));
      }

      for (String paramName : param.getLongParamNames()) {
        qparams.add(new BasicNameValuePair(
            String.format(format, SenseiSearchServletParams.PARAM_DYNAMIC_INIT, facetName, paramName, SenseiSearchServletParams.PARAM_DYNAMIC_TYPE),
            SenseiSearchServletParams.PARAM_DYNAMIC_TYPE_LONG));
        qparams.add(new BasicNameValuePair(
            String.format(format, SenseiSearchServletParams.PARAM_DYNAMIC_INIT, facetName, paramName, SenseiSearchServletParams.PARAM_DYNAMIC_VAL),
            join(param.getLongParam(paramName), ",")));
      }

      for (String paramName : param.getStringParamNames()) {
        qparams.add(new BasicNameValuePair(
            String.format(format, SenseiSearchServletParams.PARAM_DYNAMIC_INIT, facetName, paramName, SenseiSearchServletParams.PARAM_DYNAMIC_TYPE),
            SenseiSearchServletParams.PARAM_DYNAMIC_TYPE_STRING));
        qparams.add(new BasicNameValuePair(
            String.format(format, SenseiSearchServletParams.PARAM_DYNAMIC_INIT, facetName, paramName, SenseiSearchServletParams.PARAM_DYNAMIC_VAL),
            join(param.getStringParam(paramName), ",")));
      }
    }
  }

  public static void convertFacetSpecs(List<NameValuePair> qparams, Map<String,FacetSpec> facetSpecs) {
    final String format = "%s.%s.%s";

    for (Entry<String,FacetSpec> entry : facetSpecs.entrySet()) {
      String facetName = entry.getKey();
      FacetSpec spec = entry.getValue();

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

    Set<Entry<Object,Object>> entries = props.entrySet();
    for (Entry<Object,Object> entry: entries) {
      propList.add(String.format(format, entry.getKey(), entry.getValue()));
    }

    return join(propList, ",");
  }

  public static void convertSenseiQuery(List<NameValuePair> qparams, SenseiQuery query)
      throws SenseiException
  {
    if (query == null) return;

    try
    {
      JSONObject jsonObj = new FastJSONObject(query.toString());
      Iterator iter = jsonObj.keys();

      final String format = "%s:%s";

      while (iter.hasNext()) {
        String key = (String) iter.next();

        if (key.equals("query")) {
          qparams.add(new BasicNameValuePair(SenseiSearchServletParams.PARAM_QUERY, jsonObj.get(key).toString()));
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

  public URI buildSysInfoRequestURI()
      throws URISyntaxException
  {
    URI uri =
      URIUtils.createURI(
        _scheme,
        _host,
        _port,
        _path+"/sysinfo",
        null,
        null);
    return uri;
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

	if (log.isDebugEnabled()){
	  log.debug("sending: "+uri);
	}

    HttpGet httpget = new HttpGet(uri);
    HttpResponse response = _httpclient.execute(httpget);
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

  public static String join(boolean[] arr, String delimiter) {
    return join(Arrays.asList(ArrayUtils.toObject(arr)), delimiter);
  }

  public static String join(byte[] arr, String delimiter) {
    return join(Arrays.asList(ArrayUtils.toObject(arr)), delimiter);
  }

  public static String join(int[] arr, String delimiter) {
    return join(Arrays.asList(ArrayUtils.toObject(arr)), delimiter);
  }

  public static String join(long[] arr, String delimiter) {
    return join(Arrays.asList(ArrayUtils.toObject(arr)), delimiter);
  }

  public static String join(double[] arr, String delimiter) {
    return join(Arrays.asList(ArrayUtils.toObject(arr)), delimiter);
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
    char[] buf = new char[1024];  //1k buffer
     try
    {
     
      while(true){
    	  int count = reader.read(buf);
    	  if (count<0) break;
    	  sb.append(buf, 0, count);
      }
    }
    finally
    {
      is.close();
    }

    String json = sb.toString();
    if (log.isDebugEnabled()){
      log.debug("received: "+json);
    }
    return json;
  }

  public static JSONObject convertStreamToJSONObject(InputStream is)
      throws IOException, JSONException
  {
    String rawJSON = convertStreamToString(is);
    return new FastJSONObject(rawJSON);
  }

  public static SenseiResult buildSenseiResult(JSONObject jsonObj)
      throws JSONException
  {
    SenseiResult result = new SenseiResult();

    result.setTid(Long.parseLong(jsonObj.getString(SenseiSearchServletParams.PARAM_RESULT_TID)));
    result.setTotalDocs(jsonObj.getInt(SenseiSearchServletParams.PARAM_RESULT_TOTALDOCS));
    result.setParsedQuery(jsonObj.getString(SenseiSearchServletParams.PARAM_RESULT_PARSEDQUERY));
    result.setNumHits(jsonObj.getInt(SenseiSearchServletParams.PARAM_RESULT_NUMHITS));
    if (jsonObj.has(SenseiSearchServletParams.PARAM_RESULT_NUMGROUPS))
    {
      result.setNumGroups(jsonObj.getInt(SenseiSearchServletParams.PARAM_RESULT_NUMGROUPS));
    }
    result.setTime(Long.parseLong(jsonObj.getString(SenseiSearchServletParams.PARAM_RESULT_TIME)));
    result.addAll(convertFacetMap(jsonObj.getJSONObject(SenseiSearchServletParams.PARAM_RESULT_FACETS)));
    result.setHits(convertHitsArray(jsonObj.getJSONArray(SenseiSearchServletParams.PARAM_RESULT_HITS)));

    return result;
  }

  public static SenseiSystemInfo buildSysInfo(JSONObject jsonObj)
      throws JSONException
  {
    SenseiSystemInfo result = new SenseiSystemInfo();

    result.setNumDocs(jsonObj.getInt(SenseiSearchServletParams.PARAM_SYSINFO_NUMDOCS));
    result.setLastModified(Long.parseLong(jsonObj.getString(SenseiSearchServletParams.PARAM_SYSINFO_LASTMODIFIED)));
    result.setVersion(jsonObj.getString(SenseiSearchServletParams.PARAM_SYSINFO_VERSION));
    result.setFacetInfos(convertFacetInfos(jsonObj.getJSONArray(SenseiSearchServletParams.PARAM_SYSINFO_FACETS)));
    result.setClusterInfo(convertClusterInfo(jsonObj.getJSONArray(SenseiSearchServletParams.PARAM_SYSINFO_CLUSTERINFO)));

    return result;
  }

  private static Set<SenseiSystemInfo.SenseiFacetInfo> convertFacetInfos(JSONArray array)
      throws JSONException
  {
    if (array == null || array.length() == 0)
      return Collections.EMPTY_SET;

    Set<SenseiSystemInfo.SenseiFacetInfo> infos = new HashSet<SenseiSystemInfo.SenseiFacetInfo>(array.length());
    for (int i=0; i<array.length(); ++i)
    {
      JSONObject info = array.getJSONObject(i);
      SenseiSystemInfo.SenseiFacetInfo facetInfo =
        new SenseiSystemInfo.SenseiFacetInfo(info.getString(SenseiSearchServletParams.PARAM_SYSINFO_FACETS_NAME));
      facetInfo.setRunTime(info.optBoolean(SenseiSearchServletParams.PARAM_SYSINFO_FACETS_RUNTIME));
      facetInfo.setProps(convertJsonToStringMap(info.optJSONObject(SenseiSearchServletParams.PARAM_SYSINFO_FACETS_PROPS)));

      infos.add(facetInfo);
    }

    return infos;
  }

  private static Map<String, String> convertJsonToStringMap(JSONObject jsonObject)
      throws JSONException
  {
    if (jsonObject == null)
      return Collections.EMPTY_MAP;

    @SuppressWarnings("unchecked")
    Iterator<String> nameItr = jsonObject.keys();

    Map<String, String> outMap = new HashMap<String, String>();
    while(nameItr.hasNext())
    {
      String name = nameItr.next();
      outMap.put(name, jsonObject.getString(name));
    }

    return outMap;
  }

  private static List<SenseiSystemInfo.SenseiNodeInfo> convertClusterInfo(JSONArray array)
      throws JSONException
  {
    if (array == null || array.length() == 0)
      return Collections.EMPTY_LIST;

    List<SenseiSystemInfo.SenseiNodeInfo> clusterInfo = new ArrayList(array.length());
    for (int i=0; i<array.length(); ++i)
    {
      JSONObject node = array.getJSONObject(i);
      JSONArray partitionsArray = node.getJSONArray(SenseiSearchServletParams.PARAM_SYSINFO_CLUSTERINFO_PARTITIONS);
      int[] partitions = null;
      if (partitionsArray != null)
      {
        partitions = new int[partitionsArray.length()];
        for(int j=0; j<partitionsArray.length(); ++j)
        {
          partitions[j] = partitionsArray.getInt(j);
        }
      }

      clusterInfo.add(new SenseiSystemInfo.SenseiNodeInfo(
        node.getInt(SenseiSearchServletParams.PARAM_SYSINFO_CLUSTERINFO_ID),
        partitions,
        node.getString(SenseiSearchServletParams.PARAM_SYSINFO_CLUSTERINFO_NODELINK),
        node.getString(SenseiSearchServletParams.PARAM_SYSINFO_CLUSTERINFO_ADMINLINK)
      ));
    }

    return clusterInfo;
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
        bf.setFacetValueHitCount(facetObj.getInt(SenseiSearchServletParams.PARAM_RESULT_FACET_INFO_COUNT));
        bf.setValue(facetObj.getString(SenseiSearchServletParams.PARAM_RESULT_FACET_INFO_VALUE));
        facets[i] = bf;
      }

      FacetAccessible fa = new MappedFacetAccessible(facets);

      map.put(fieldName, fa);
    }

    return map;
  }

  private static SenseiHit[] convertHitsArray(JSONArray hitsArray)
      throws JSONException
  {
    int hitsArrayLength = hitsArray.length();

    SenseiHit[] result = new SenseiHit[hitsArrayLength];

    for (int i = 0; i < hitsArrayLength; i++) {
      JSONObject hitObj = (JSONObject)hitsArray.get(i);

      SenseiHit hit = new SenseiHit();
      Iterator keys = hitObj.keys();
      Map<String,String[]> fieldMap = new HashMap<String,String[]>();
      while(keys.hasNext()){
    	  String key = (String)keys.next();
    	  if (SenseiSearchServletParams.PARAM_RESULT_HIT_UID.equals(key)){
    		  hit.setUID(Long.parseLong(hitObj.getString(SenseiSearchServletParams.PARAM_RESULT_HIT_UID)));
    	  }
    	  else if (SenseiSearchServletParams.PARAM_RESULT_HIT_DOCID.equals(key)){
    		  hit.setDocid(hitObj.getInt(SenseiSearchServletParams.PARAM_RESULT_HIT_DOCID));
    	  }
    	  else if (SenseiSearchServletParams.PARAM_RESULT_HIT_SCORE.equals(key)){
    		  hit.setScore((float) hitObj.getDouble(SenseiSearchServletParams.PARAM_RESULT_HIT_SCORE));
    	  }
    	  else if (SenseiSearchServletParams.PARAM_RESULT_HIT_SRC_DATA.equals(key)){
    		  hit.setSrcData(hitObj.getString(SenseiSearchServletParams.PARAM_RESULT_HIT_SRC_DATA));
    	  }
    	  else if (SenseiSearchServletParams.PARAM_RESULT_HIT_STORED_FIELDS.equals(key)){
    		  hit.setStoredFields(convertStoredFields(hitObj.optJSONArray(SenseiSearchServletParams.PARAM_RESULT_HIT_STORED_FIELDS)));
    	  }
    	  else if (SenseiSearchServletParams.PARAM_RESULT_HIT_GROUPFIELD.equals(key)){
    		  hit.setGroupValue(hitObj.getString(SenseiSearchServletParams.PARAM_RESULT_HIT_GROUPFIELD));
    	  }
    	  else if (SenseiSearchServletParams.PARAM_RESULT_HIT_GROUPVALUE.equals(key)){
    		  hit.setGroupValue(hitObj.getString(SenseiSearchServletParams.PARAM_RESULT_HIT_GROUPVALUE));
    	  }
    	  else if (SenseiSearchServletParams.PARAM_RESULT_HIT_GROUPHITSCOUNT.equals(key)){
    		  hit.setGroupHitsCount(hitObj.getInt(SenseiSearchServletParams.PARAM_RESULT_HIT_GROUPHITSCOUNT));
    	  }
    	  else if (SenseiSearchServletParams.PARAM_RESULT_HIT_EXPLANATION.equals(key)){
    		  hit.setExplanation(convertToExplanation(hitObj.optJSONObject(SenseiSearchServletParams.PARAM_RESULT_HIT_EXPLANATION)));
    	  }
        else if (SenseiSearchServletParams.PARAM_RESULT_HIT_GROUPHITS.equals(key)) {
          hit.setGroupHits(convertHitsArray(hitObj.getJSONArray(SenseiSearchServletParams.PARAM_RESULT_HIT_GROUPHITS)));
        }
    	  else{
    		  JSONArray array = hitObj.optJSONArray(key);
    		  if (array!=null){
    			  String [] arr = new String[array.length()];
    			  for (int k=0;k<arr.length;++k){
    				  arr[k]=array.getString(k);
    			  }
        		  fieldMap.put(key, arr);  
    		  }
    	  }
      }
      
      hit.setFieldValues(fieldMap);
      //hit.setFieldValues(convertRawFieldValues());

      result[i] = hit;
    }

    return result;
  }
 

  public static Document convertStoredFields(JSONArray jsonArray)
      throws JSONException
  {
    int length = jsonArray.length();

    Document doc = new Document();

    for (int i = 0; i < length; i++) {
      JSONObject jsonObject = (JSONObject) jsonArray.get(i);
      String name = jsonObject.getString(SenseiSearchServletParams.PARAM_RESULT_HIT_STORED_FIELDS_NAME);
      String value = jsonObject.getString(SenseiSearchServletParams.PARAM_RESULT_HIT_STORED_FIELDS_VALUE);
      doc.add(new org.apache.lucene.document.Field(name, value, Field.Store.YES, Field.Index.ANALYZED));
    }

    return doc;
  }

  public static Explanation convertToExplanation(JSONObject jsonObj)
      throws JSONException
  {
	if (jsonObj == null) return null;
    Explanation explanation = new Explanation();

    float value = (float) jsonObj.optDouble(SenseiSearchServletParams.PARAM_RESULT_HITS_EXPL_VALUE);
    String description = jsonObj.optString(SenseiSearchServletParams.PARAM_RESULT_HITS_EXPL_DESC);

    explanation.setDescription(description);
    explanation.setValue(value);

    if (jsonObj.has(SenseiSearchServletParams.PARAM_RESULT_HITS_EXPL_DETAILS)) {
      JSONArray detailsArr = jsonObj.getJSONArray(SenseiSearchServletParams.PARAM_RESULT_HITS_EXPL_DETAILS);
      int detailsCnt = detailsArr.length();

      for (int i = 0; i < detailsCnt; i++) {
        JSONObject detailObj = (JSONObject) detailsArr.get(i);
        Explanation detailExpl = convertToExplanation(detailObj);
        explanation.addDetail(detailExpl);
      }
    }

    return explanation;
  }

  @Override
  public void shutdown() {
    if (_httpclient == null) return;
    _httpclient.getConnectionManager().shutdown();
    _httpclient = null;
  }

}
