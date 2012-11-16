package com.senseidb.servlet;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.antlr.runtime.RecognitionException;
import org.apache.commons.configuration.DataConfiguration;
import org.apache.commons.configuration.MapConfiguration;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.browseengine.bobo.api.BrowseSelection;
import com.linkedin.norbert.javacompat.cluster.ClusterClient;
import com.linkedin.norbert.javacompat.network.NetworkClientConfig;
import com.senseidb.bql.parsers.BQLCompiler;
import com.senseidb.cluster.client.SenseiNetworkClient;
import com.senseidb.conf.SenseiConfParams;
import com.senseidb.conf.SenseiFacetHandlerBuilder;
import com.senseidb.search.node.Broker;
import com.senseidb.search.node.SenseiBroker;
import com.senseidb.search.node.SenseiSysBroker;
import com.senseidb.search.node.broker.BrokerConfig;
import com.senseidb.search.node.broker.LayeredBroker;
import com.senseidb.search.req.ErrorType;
import com.senseidb.search.req.SenseiError;
import com.senseidb.search.req.SenseiHit;
import com.senseidb.search.req.SenseiJSONQuery;
import com.senseidb.search.req.SenseiRequest;
import com.senseidb.search.req.SenseiResult;
import com.senseidb.search.req.SenseiSystemInfo;
import com.senseidb.svc.api.SenseiException;
import com.senseidb.svc.impl.HttpRestSenseiServiceImpl;
import com.senseidb.util.JsonTemplateProcessor;
import com.senseidb.util.RequestConverter2;
import com.senseidb.util.JSONUtil.FastJSONArray;
import com.senseidb.util.JSONUtil.FastJSONObject;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.MetricName;

public abstract class AbstractSenseiClientServlet extends ZookeeperConfigurableServlet {

  public static final int JSON_PARSING_ERROR = 489;
  public static final int BQL_EXTRA_FILTER_ERROR = 498;
  public static final int BQL_PARSING_ERROR = 499;
  public static final String BQL_STMT = "bql";
  public static final String BQL_EXTRA_FILTER = "bql_extra_filter";
  public static final String TOTAL_DOCS = "totaldocs";
  private static final long serialVersionUID = 1L;

  private static final Logger logger = Logger.getLogger(AbstractSenseiClientServlet.class);
  private static final Logger queryLogger = Logger.getLogger("com.sensei.querylog");
  private static final Counter totalDocsCounter =
      Metrics.newCounter(new MetricName(AbstractSenseiClientServlet.class,
                                        TOTAL_DOCS));

  private final NetworkClientConfig _networkClientConfig = new NetworkClientConfig();

  private ClusterClient _clusterClient = null;
  private SenseiNetworkClient _networkClient = null;
  private SenseiBroker _senseiBroker = null;
  private SenseiSysBroker _senseiSysBroker = null;
  private Map<String, String[]> _facetInfoMap = new HashMap<String, String[]>();
  private BQLCompiler _compiler = null;
  private LayeredBroker federatedBroker;

  private JsonTemplateProcessor jsonTemplateProcessor = new JsonTemplateProcessor();

  private Timer _statTimer;


  public AbstractSenseiClientServlet() {
    _statTimer = new Timer(true);
  }

  @Override
  public void init(ServletConfig config) throws ServletException {
    super.init(config);
    BrokerConfig brokerConfig = new BrokerConfig(senseiConf, loadBalancerFactory);
    brokerConfig.init();
    _senseiBroker = brokerConfig.buildSenseiBroker();
    _senseiSysBroker = brokerConfig.buildSysSenseiBroker(versionComparator);
    _networkClient = brokerConfig.getNetworkClient();
    _clusterClient = brokerConfig.getClusterClient();
    federatedBroker = pluginRegistry.getBeanByFullPrefix(SenseiConfParams.SENSEI_FEDERATED_BROKER, LayeredBroker.class);
    if (federatedBroker != null) { 
      federatedBroker.warmUp();
    }
    logger.info("Connecting to cluster: " + brokerConfig.getClusterName() +" ...");
    _clusterClient.awaitConnectionUninterruptibly();

    int count = 0;
    while (true)
    {
      try
      {
        count++;
        logger.info("Trying to get sysinfo");
        SenseiSystemInfo sysInfo = _senseiSysBroker.browse(new SenseiRequest());

        _facetInfoMap = sysInfo != null && sysInfo.getFacetInfos() != null ? extractFacetInfo(sysInfo) : new HashMap<String, String[]>();
        _compiler = new BQLCompiler(_facetInfoMap);
        break;
      }
      catch (Exception e)
      {
        logger.info("Hit exception trying to get sysinfo", e);
        if (count > 10) 
        {
          logger.error("Give up after 10 tries to get sysinfo");
          throw new ServletException(e.getMessage(), e);
        }
        else
        {
          try
          {
            Thread.sleep(2000);
          }
          catch (InterruptedException e2)
          {
            logger.error("Hit InterruptedException in getting sysinfo: ", e);
          }
        }
      }
    }

    // Start the stat timer to get some of the sys stat:
    _statTimer.scheduleAtFixedRate(new TimerTask()
    {
      public void run()
      {
        int totalDocs = 0;
        try
        {
          SenseiRequest req = new SenseiRequest();
          req.setQuery(new SenseiJSONQuery(new FastJSONObject().put("query", "dummy:dummy")));
          SenseiResult res = _senseiBroker.browse(req);
          totalDocs = res.getTotalDocs();
        }
        catch(Exception e)
        {
          logger.warn("Error getting result", e);
        }
        if (totalDocs > 0)
        {
          totalDocsCounter.clear();
          totalDocsCounter.inc(totalDocs);
        }
        else
        {
          logger.warn("Unable to get total docs");
        }

        try
        {
          SenseiSystemInfo sysInfo = _senseiSysBroker.browse(new SenseiRequest());

          if (sysInfo != null && sysInfo.getFacetInfos() != null)
          {
            _facetInfoMap = extractFacetInfo(sysInfo);
            _compiler.setFacetInfoMap(_facetInfoMap);
          }
        }
        catch (Exception e)
        {
          logger.info("Hit exception trying to get sysinfo", e);
        }
      }
    }, 60000, 60000); // Every minute.

    logger.info("Cluster: "+ brokerConfig.getClusterName() +" successfully connected ");
  }

  public static Map<String, String[]> extractFacetInfo(SenseiSystemInfo sysInfo) {
    Map<String, String[]> facetInfoMap = new HashMap<String, String[]>();
    Iterator<SenseiSystemInfo.SenseiFacetInfo> itr = sysInfo.getFacetInfos().iterator();
    while (itr.hasNext())
    {
      SenseiSystemInfo.SenseiFacetInfo facetInfo = itr.next();
      Map<String, String> props = facetInfo.getProps();
      facetInfoMap.put(facetInfo.getName(), new String[]{props.get("type"), props.get("column_type")});
    }
    return facetInfoMap;
  }

  protected abstract SenseiRequest buildSenseiRequest(HttpServletRequest req) throws Exception;

  public static Map<String, String> getParameters(String query)
      throws Exception {
    Map<String, String> params = new HashMap<String, String>();
    for (String param : query.split("&")) {
      String pair[] = param.split("=");
      String key = URLDecoder.decode(pair[0], "UTF-8");
      String value = "";
      if (pair.length > 1) {
        value = URLDecoder.decode(pair[1], "UTF-8");
      }
      params.put(key, value);
    }
    return params;
  }
  private static class RequestContext {
    String query;
    JSONObject jsonObj;
    public String bqlStmt;
    public JSONObject templatesJson;
    public JSONObject compiledJson;
    public String content;
    public SenseiRequest senseiReq;
  }
  private void handleSenseiRequest(HttpServletRequest req, HttpServletResponse resp, Broker<SenseiRequest, SenseiResult> broker)
      throws ServletException, IOException {
    long time = System.currentTimeMillis();
    int numHits = 0, totalDocs = 0;
    RequestContext requestContext = null;
    try
    {
      if ("post".equalsIgnoreCase(req.getMethod()))
      {
        requestContext = initializeRequestContextBasedOnPostParams(req, resp);
      }
      else
      {
        requestContext = initContextBasedOnGetParams(req, resp);
      }
      if (requestContext == null) {
        //the error has been already logged
        return;
      }
      if (requestContext.jsonObj != null)
      {
        requestContext.bqlStmt = requestContext.jsonObj.optString(BQL_STMT);
        requestContext.templatesJson = requestContext.jsonObj.optJSONObject(JsonTemplateProcessor.TEMPLATE_MAPPING_PARAM);
        requestContext.compiledJson = null;

        if (requestContext.bqlStmt.length() > 0)
        {
          boolean successfull = handleBqlRequest(req, resp, requestContext);
          if (!successfull) {
            return;
          }
        }
        else
        {
          // This is NOT a BQL statement
          requestContext.query = "json=" + requestContext.content;
          requestContext.compiledJson = requestContext.jsonObj;
        }

        if (requestContext.templatesJson != null)
        {
          requestContext.compiledJson.put(JsonTemplateProcessor.TEMPLATE_MAPPING_PARAM, requestContext.templatesJson);
        }
        requestContext.senseiReq = SenseiRequest.fromJSON(requestContext.compiledJson, _facetInfoMap);
      }
      SenseiResult res = broker.browse(requestContext.senseiReq);
      numHits = res.getNumHits();
      totalDocs = res.getTotalDocs();
      sendResponse(req, resp, requestContext.senseiReq, res);
   } catch (JSONException e) {
      try {
        writeEmptyResponse(req, resp, new SenseiError(e.getMessage(), ErrorType.JsonParsingError));
      } catch (Exception ex) {
        throw new ServletException(e);
      }
    }
    catch (Exception e)
    {
      try {
        logger.error(e.getMessage(), e);
        if (e.getCause() != null && e.getCause() instanceof JSONException) {
          writeEmptyResponse(req, resp, new SenseiError(e.getMessage(), ErrorType.JsonParsingError));
      } else {
        writeEmptyResponse(req, resp, new SenseiError(e.getMessage(), ErrorType.InternalError));
      }
      } catch (Exception ex) {
        throw new ServletException(e);
      }
    }
    finally
    {
      if (queryLogger.isInfoEnabled() && requestContext != null && requestContext.query != null)
      {
        queryLogger.info(String.format("hits(%d/%d) took %dms: %s", numHits, totalDocs, System.currentTimeMillis() - time, requestContext.query));
      }
    }
  }

  public RequestContext initContextBasedOnGetParams(HttpServletRequest req, HttpServletResponse resp) throws Exception,
      SenseiException, UnsupportedEncodingException {
    RequestContext requestContext;
    requestContext = new RequestContext();
    requestContext.content = req.getParameter("json");
    if (requestContext.content != null)
    {
      if (requestContext.content.length() == 0) requestContext.content = "{}";
      try
      {
        requestContext.jsonObj = new FastJSONObject(requestContext.content);
      }
      catch(JSONException jse)
      {
        logger.error("JSON parsing error", jse);
        writeEmptyResponse(req, resp, new SenseiError(jse.getMessage(), ErrorType.JsonParsingError));
        return null;
      }
    }
    else
    {
      requestContext.senseiReq = buildSenseiRequest(req);
      requestContext.query = URLEncodedUtils.format(
                HttpRestSenseiServiceImpl.convertRequestToQueryParams(requestContext.senseiReq), "UTF-8");
    }
    return requestContext;
  }

  public RequestContext initializeRequestContextBasedOnPostParams(HttpServletRequest req, HttpServletResponse resp)
      throws IOException, Exception {
    RequestContext requestContext;
    requestContext = new RequestContext();
    BufferedReader reader = req.getReader();
    requestContext.content = readContent(reader);
    if (requestContext.content == null || requestContext.content.length() == 0) requestContext.content = "{}";
    try
    {
      requestContext.jsonObj = new FastJSONObject(requestContext.content);
    }
    catch(JSONException jse)
    {
      String contentType = req.getHeader("Content-Type");
      if (contentType != null && contentType.indexOf("json") >= 0)
      {
        logger.error("JSON parsing error", jse);           
          writeEmptyResponse(req, resp, new SenseiError(jse.getMessage(), ErrorType.JsonParsingError));              
          return null;            
      }

      logger.warn("Old client or json error", jse);

      // Fall back to the old REST API.  In the future, we should
      // consider reporting JSON exceptions here.
      requestContext.senseiReq = DefaultSenseiJSONServlet.convertSenseiRequest(
                    new DataConfiguration(new MapConfiguration(getParameters(requestContext.content))));
      requestContext.query = requestContext.content;
    }
    return requestContext;
  }

  public boolean handleBqlRequest(HttpServletRequest req, HttpServletResponse resp, RequestContext requestContext) throws Exception,
      JSONException {
    try
    {
      if (requestContext.jsonObj.length() == 1)
        requestContext.query = "bql=" + requestContext.bqlStmt;
      else
        requestContext.query = "json=" + requestContext.content;
      // Disable variables replacing before bql compling, since that data representation in json and bql is quite different for now.
      //requestContext.bqlStmt = (String) jsonTemplateProcessor.process(requestContext.bqlStmt, jsonTemplateProcessor.getTemplates(requestContext.jsonObj));
      requestContext.compiledJson = _compiler.compile(requestContext.bqlStmt);
    }
    catch (RecognitionException e)
    {
      String errMsg = _compiler.getErrorMessage(e);
      if (errMsg == null) 
      {
        errMsg = "Unknown parsing error.";
      }
      logger.error("BQL parsing error: " + errMsg + ", BQL: " + requestContext.bqlStmt);
      writeEmptyResponse(req, resp, new SenseiError(errMsg, ErrorType.BQLParsingError));
      return false;
    }

    // Handle extra BQL filter if it exists
    String extraFilter = requestContext.jsonObj.optString(BQL_EXTRA_FILTER);
    JSONObject predObj = null;
    if (extraFilter.length() > 0)
    {
      String bql2 = "SELECT * WHERE " + extraFilter;
      try
      {
        predObj = _compiler.compile(bql2);
      }
      catch (RecognitionException e)
      {
        String errMsg = _compiler.getErrorMessage(e);
        if (errMsg == null) 
        {
          errMsg = "Unknown parsing error.";
        }
        logger.error("BQL parsing error for additional preds: " + errMsg + ", BQL: " + bql2);
        writeEmptyResponse(req, resp, new SenseiError("BQL parsing error for additional preds: " + errMsg + ", BQL: " + bql2, ErrorType.BQLParsingError));
        return false;
      }

      // Combine filters
      JSONArray filter_list = new FastJSONArray();
      JSONObject currentFilter = requestContext.compiledJson.optJSONObject("filter");
      if (currentFilter != null)
      {
        filter_list.put(currentFilter);
      }

      JSONArray selections = predObj.optJSONArray("selections");
      if (selections != null)
      {
        for (int i = 0; i < selections.length(); ++i)
        {
          JSONObject pred = selections.getJSONObject(i);
          if (pred != null)
          {
            filter_list.put(pred);
          }
        }
      }
      JSONObject additionalFilter = predObj.optJSONObject("filter");
      if (additionalFilter != null)
      {
        filter_list.put(additionalFilter);
      }
      
      if (filter_list.length() > 1)
      {
        requestContext.compiledJson.put("filter", new FastJSONObject().put("and", filter_list));
      }
      else if (filter_list.length() == 1)
      {
        requestContext.compiledJson.put("filter", filter_list.get(0));
      }
    }

    JSONObject metaData = requestContext.compiledJson.optJSONObject("meta");
    if (metaData != null)
    {
      JSONArray variables = metaData.optJSONArray("variables");
      if (variables != null)
      {
        for (int i = 0; i < variables.length(); ++i)
        {
          String var = variables.getString(i);
          if (requestContext.templatesJson == null ||
              requestContext.templatesJson.opt(var) == null)
          {
            writeEmptyResponse(req, resp, new SenseiError("[line:0, col:0] Variable " + var + " is not found.", ErrorType.BQLParsingError));
            return false;
          }
        }
      }
    }
    return true;
  }

  private void writeEmptyResponse(HttpServletRequest req, HttpServletResponse resp, SenseiError senseiError) throws Exception {
    SenseiResult res = new SenseiResult();
    res.addError(senseiError);
    sendResponse(req, resp, new SenseiRequest(), res);
  }

  private void sendResponse(HttpServletRequest req, HttpServletResponse resp, SenseiRequest senseiReq, SenseiResult res) throws Exception {
    OutputStream ostream = resp.getOutputStream();
    convertResult(req, senseiReq, res, ostream);
    ostream.flush();
  }

  private void handleStoreGetRequest(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    long time = System.currentTimeMillis();
    int numHits = 0, totalDocs = 0;
    String query = null;

    SenseiRequest senseiReq = null;
    try
    {
      JSONArray ids = null;
      if ("post".equalsIgnoreCase(req.getMethod()))
      {
        BufferedReader reader = req.getReader();
        ids = new FastJSONArray(readContent(reader));
      }
      else
      {
        String jsonString = req.getParameter("json");
        if (jsonString != null)
          ids = new FastJSONArray(jsonString);
      }

      query = "get=" + String.valueOf(ids);

      String[] vals = RequestConverter2.getStrings(ids);
      if (vals != null && vals.length != 0)
      {
        senseiReq = new SenseiRequest();
        senseiReq.setFetchStoredValue(true);
        senseiReq.setCount(vals.length);
        BrowseSelection sel = new BrowseSelection(SenseiFacetHandlerBuilder.UID_FACET_NAME);
        sel.setValues(vals);
        senseiReq.addSelection(sel);
      }

      SenseiResult res = null;
      if (senseiReq != null)
        res =_senseiBroker.browse(senseiReq);

      if (res != null)
      {
        numHits = res.getNumHits();
        totalDocs = res.getTotalDocs();
      }

      JSONObject ret = new FastJSONObject();
      JSONObject obj = null;
      if (res != null && res.getSenseiHits() != null)
      {
        for (SenseiHit hit : res.getSenseiHits())
        {
          try
          {
            obj = new FastJSONObject(hit.getSrcData());
            ret.put(String.valueOf(hit.getUID()), obj);
          }
          catch(Exception ex)
          {
            logger.warn(ex.getMessage(), ex);
          }
        }
      }
      OutputStream ostream = resp.getOutputStream();
      ostream.write(ret.toString().getBytes("UTF-8"));
      ostream.flush();
    }
    catch (Exception e)
    {
      throw new ServletException(e.getMessage(),e);
    }
    finally
    {
      if (queryLogger.isInfoEnabled() && query != null)
      {
        queryLogger.info(String.format("hits(%d/%d) took %dms: %s", numHits, totalDocs, System.currentTimeMillis() - time, query));
      }
    }
  }

  private void handleSystemInfoRequest(HttpServletRequest req, HttpServletResponse resp)
    throws ServletException, IOException {
    try {
      SenseiSystemInfo res = _senseiSysBroker.browse(new SenseiRequest());
      OutputStream ostream = resp.getOutputStream();
      convertResult(req, res, ostream);
      ostream.flush();
    } catch (Exception e) {
      throw new ServletException(e.getMessage(),e);
    }
  }

  private void handleJMXRequest(HttpServletRequest req, HttpServletResponse resp)
    throws ServletException, IOException
  {
    InputStream is = null;
    OutputStream os = null;
    try
    {
      String myPath = req.getRequestURI().substring(req.getServletPath().length()+11);
      URL adminUrl = null;
      if (myPath.indexOf('/') > 0)
      {
        adminUrl = new URL(new StringBuilder(URLDecoder.decode(myPath.substring(0, myPath.indexOf('/')), "UTF-8"))
          .append("/admin/jmx")
          .append(myPath.substring(myPath.indexOf('/'))).toString());
      }
      else
      {
        adminUrl = new URL(new StringBuilder(URLDecoder.decode(myPath, "UTF-8"))
          .append("/admin/jmx").toString());
      }

      URLConnection conn = adminUrl.openConnection();

      byte[] buffer = new byte[8192]; // 8k
      int len = 0;

      InputStream ris = req.getInputStream();

      while((len=ris.read(buffer)) > 0)
      {
        if (!conn.getDoOutput()) {
          conn.setDoOutput(true);
          os = conn.getOutputStream();
        }
        os.write(buffer, 0, len);
      }
      if (os != null)
        os.flush();

      is = conn.getInputStream();
      OutputStream ros = resp.getOutputStream();

      while((len=is.read(buffer)) > 0)
      {
        ros.write(buffer, 0, len);
      }
      ros.flush();
    }
    catch (Exception e)
    {
      throw new ServletException(e.getMessage(),e);
    }
    finally
    {
      if (is != null)
        is.close();
      if (os != null)
        os.close();
    }
  }

  private static String readContent(BufferedReader reader) throws IOException{
	  StringBuilder jb = new StringBuilder();
      String line = null;
	  while ((line = reader.readLine()) != null)
		jb.append(line);
	  return jb.toString();
  }

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    if (req.getCharacterEncoding() == null)
      req.setCharacterEncoding("UTF-8");
    resp.setContentType("application/json; charset=utf-8");
    resp.setCharacterEncoding("UTF-8");

    resp.setHeader("Access-Control-Allow-Origin", "*");
    resp.setHeader("Access-Control-Allow-Methods", "GET, POST");
    resp.setHeader("Access-Control-Allow-Headers", "Origin, Content-Type, X-Requested-With, Accept");

    if (null == req.getPathInfo() || "/".equalsIgnoreCase(req.getPathInfo()))
    {
      handleSenseiRequest(req, resp, _senseiBroker);
    }
    else if ("/get".equalsIgnoreCase(req.getPathInfo()))
    {
      handleStoreGetRequest(req, resp);
    }
    else if ("/sysinfo".equalsIgnoreCase(req.getPathInfo()))
    {
      handleSystemInfoRequest(req, resp);
    }
    else if (req.getPathInfo().startsWith("/admin/jmx/"))
    {
      handleJMXRequest(req, resp);
    }else if (req.getPathInfo().startsWith("/federatedBroker/"))
    {
      if (federatedBroker == null) {
        try {
          writeEmptyResponse(req, resp, new SenseiError("The federated broker wasn't initialized", ErrorType.FederatedBrokerUnavailable)) ;
        } catch (Exception e) {
          throw new RuntimeException(e);
        }                    
      }
      handleSenseiRequest(req, resp, federatedBroker);
    }
    else
    {
      handleSenseiRequest(req, resp, _senseiBroker);
    }
  }

  @Override
  protected void doPost(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException
  {
    doGet(req, resp);
  }

  @Override
  protected void doOptions(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException
  {
    resp.setHeader("Access-Control-Allow-Origin", "*");
    resp.setHeader("Access-Control-Allow-Methods", "GET, POST");
    resp.setHeader("Access-Control-Allow-Headers", "Origin, Content-Type, X-Requested-With, Accept");
  }

  protected abstract void convertResult(HttpServletRequest httpReq, SenseiSystemInfo info, OutputStream ostream) throws Exception;

  protected abstract void convertResult(HttpServletRequest httpReq, SenseiRequest req,SenseiResult res,OutputStream ostream) throws Exception;

  @Override
  public void destroy() {
    try{
      try{
        if (_senseiBroker!=null){
          _senseiBroker.shutdown();
          _senseiBroker = null;
        }
      }
      finally{
        try {
          if (_senseiSysBroker!=null){
            _senseiSysBroker.shutdown();
            _senseiSysBroker = null;
          }
        }
        finally
        {
          try{
            if (_networkClient!=null){
              _networkClient.shutdown();
              _networkClient = null;
            }
          }
          finally{
            try
            {
              if (_clusterClient!=null)
              {
                _clusterClient.shutdown();
                _clusterClient = null;
              }
            }
            finally
            {
              _statTimer.cancel();
            }
          }
        }
      }

    }
    finally{
      super.destroy();
    }
  }
}
