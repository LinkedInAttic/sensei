package com.senseidb.servlet;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

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
import com.linkedin.norbert.javacompat.cluster.ZooKeeperClusterClient;
import com.linkedin.norbert.javacompat.network.NetworkClientConfig;
import com.senseidb.bql.parsers.BQLCompiler;
import com.senseidb.cluster.client.SenseiNetworkClient;
import com.senseidb.conf.SenseiFacetHandlerBuilder;
import com.senseidb.search.node.SenseiBroker;
import com.senseidb.search.node.SenseiSysBroker;
import com.senseidb.search.req.SenseiHit;
import com.senseidb.search.req.SenseiRequest;
import com.senseidb.search.req.SenseiResult;
import com.senseidb.search.req.SenseiSystemInfo;
import com.senseidb.svc.impl.HttpRestSenseiServiceImpl;
import com.senseidb.util.JsonTemplateProcessor;
import com.senseidb.util.RequestConverter2;

public abstract class AbstractSenseiClientServlet extends ZookeeperConfigurableServlet {

  public static final int JSON_PARSING_ERROR = 489;
  public static final int BQL_PARSING_ERROR = 499;
  private static final long serialVersionUID = 1L;

  private static final Logger logger = Logger.getLogger(AbstractSenseiClientServlet.class);
  private static final Logger queryLogger = Logger.getLogger("com.sensei.querylog");

  private final NetworkClientConfig _networkClientConfig = new NetworkClientConfig();

  private ClusterClient _clusterClient = null;
  private SenseiNetworkClient _networkClient = null;
  private SenseiBroker _senseiBroker = null;
  private SenseiSysBroker _senseiSysBroker = null;
  private BQLCompiler _compiler = null;

  public AbstractSenseiClientServlet() {

  }

  @Override
  public void init(ServletConfig config) throws ServletException {
    super.init(config);
    _networkClientConfig.setServiceName(clusterName);
    _networkClientConfig.setZooKeeperConnectString(zkurl);
    _networkClientConfig.setZooKeeperSessionTimeoutMillis(zkTimeout);
    _networkClientConfig.setConnectTimeoutMillis(connectTimeoutMillis);
    _networkClientConfig.setWriteTimeoutMillis(writeTimeoutMillis);
    _networkClientConfig.setMaxConnectionsPerNode(maxConnectionsPerNode);
    _networkClientConfig.setStaleRequestTimeoutMins(staleRequestTimeoutMins);
    _networkClientConfig.setStaleRequestCleanupFrequencyMins(staleRequestCleanupFrequencyMins);

    _clusterClient = new ZooKeeperClusterClient(clusterName,zkurl,zkTimeout);

    _networkClientConfig.setClusterClient(_clusterClient);

    _networkClient = new SenseiNetworkClient(_networkClientConfig, null);
    _senseiBroker = new SenseiBroker(_networkClient, _clusterClient, loadBalancerFactory);
    _senseiSysBroker = new SenseiSysBroker(_networkClient, _clusterClient, loadBalancerFactory, versionComparator);

    logger.info("Connecting to cluster: "+clusterName+" ...");
    _clusterClient.awaitConnectionUninterruptibly();

    int count = 0;
    while (true)
    {
      try
      {
        count++;
        logger.info("Trying to get sysinfo");
        SenseiSystemInfo sysInfo = _senseiSysBroker.browse(new SenseiRequest());

        Map<String, String[]> facetInfoMap = new HashMap<String, String[]>();
        Iterator<SenseiSystemInfo.SenseiFacetInfo> itr = sysInfo.getFacetInfos().iterator();
        while (itr.hasNext())
        {
          SenseiSystemInfo.SenseiFacetInfo facetInfo = itr.next();
          Map<String, String> props = facetInfo.getProps();
          facetInfoMap.put(facetInfo.getName(), new String[]{props.get("type"), props.get("column_type")});
        }
        _compiler = new BQLCompiler(facetInfoMap);
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
    logger.info("Cluster: "+clusterName+" successfully connected ");
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

  private void handleSenseiRequest(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    SenseiRequest senseiReq = null;
    try
    {
      JSONObject jsonObj = null;
      String content = null;

      if ("post".equalsIgnoreCase(req.getMethod()))
      {
        BufferedReader reader = req.getReader();
        content = readContent(reader);
        if (content == null || content.length() == 0) content = "{}";
        try
        {
          jsonObj = new JSONObject(content);
        }
        catch(JSONException jse)
        {
          String contentType = req.getHeader("Content-Type");
          if (contentType != null && contentType.indexOf("json") >= 0)
          {
            logger.error("JSON parsing error", jse);
            OutputStream ostream = resp.getOutputStream();
            try
            {
              JSONObject errResp = new JSONObject().put("error",
                                                        new JSONObject().put("code", JSON_PARSING_ERROR)
                                                                        .put("msg", jse.getMessage()));
              ostream.write(errResp.toString().getBytes("UTF-8"));
              ostream.flush();
              return;
            }
            catch (JSONException err)
            {
              logger.error(err.getMessage());
              throw new ServletException(err.getMessage(), err);
            }
          }

          logger.warn("Old client or json error", jse);

          // Fall back to the old REST API.  In the future, we should
          // consider reporting JSON exceptions here.
          senseiReq = DefaultSenseiJSONServlet.convertSenseiRequest(
                        new DataConfiguration(new MapConfiguration(getParameters(content))));
          if (queryLogger.isInfoEnabled()){
            queryLogger.info(content);
          }
        }
      }
      else
      {
        content = req.getParameter("json");
        if (content != null)
        {
          if (content.length() == 0) content = "{}";
          try
          {
            jsonObj = new JSONObject(content);
          }
          catch(JSONException jse)
          {
            logger.error("JSON parsing error", jse);
            OutputStream ostream = resp.getOutputStream();
            try
            {
              JSONObject errResp = new JSONObject().put("error",
                                                        new JSONObject().put("code", JSON_PARSING_ERROR)
                                                                        .put("msg", jse.getMessage()));
              ostream.write(errResp.toString().getBytes("UTF-8"));
              ostream.flush();
              return;
            }
            catch (JSONException err)
            {
              logger.error(err.getMessage());
              throw new ServletException(err.getMessage(), err);
            }
          }
        }
        else
        {
          senseiReq = buildSenseiRequest(req);
          if (queryLogger.isInfoEnabled()){
            queryLogger.info(
                URLEncodedUtils.format(
                    HttpRestSenseiServiceImpl.convertRequestToQueryParams(senseiReq), "UTF-8"));
          }
        }
      }

      if (jsonObj != null)
      {
        String bqlStmt = jsonObj.optString("bql");
        JSONObject templatesJson = jsonObj.optJSONObject(JsonTemplateProcessor.TEMPLATE_MAPPING_PARAM);
        if (bqlStmt.length() > 0)
        {
          try 
          {
            if (queryLogger.isInfoEnabled()){
              queryLogger.info("bql=" + bqlStmt);
            }
            jsonObj = _compiler.compile(bqlStmt);
          }
          catch (RecognitionException e)
          {
            String errMsg = _compiler.getErrorMessage(e);
            if (errMsg == null) 
            {
              errMsg = "Unknown parsing error.";
            }
            logger.error("BQL parsing error: " + errMsg + ", BQL: " + bqlStmt);
            OutputStream ostream = resp.getOutputStream();
            try
            {
              JSONObject errResp = new JSONObject().put("error",
                                                        new JSONObject().put("code", BQL_PARSING_ERROR)
                                                                        .put("msg", errMsg));
              ostream.write(errResp.toString().getBytes("UTF-8"));
              ostream.flush();
              return;
            }
            catch (JSONException err)
            {
              logger.error(err.getMessage());
              throw new ServletException(err.getMessage(), err);
            }
          }

          JSONObject metaData = jsonObj.optJSONObject("meta");
          if (metaData != null)
          {
            JSONArray variables = metaData.optJSONArray("variables");
            if (variables != null)
            {
              for (int i = 0; i < variables.length(); ++i)
              {
                String var = variables.getString(i);
                if (templatesJson == null ||
                    templatesJson.opt(var) == null)
                {
                  OutputStream ostream = resp.getOutputStream();
                  JSONObject errResp = new JSONObject().put("error",
                                                            new JSONObject().put("code", BQL_PARSING_ERROR)
                                                                            .put("msg", "[line:0, col:0] Variable " + var + " is not found."));
                  ostream.write(errResp.toString().getBytes("UTF-8"));
                  ostream.flush();
                  return;
                }
              }
            }
          }
        }
        else
        {
          if (queryLogger.isInfoEnabled()){
            queryLogger.info("query=" + content);
          }
        }

        if (templatesJson != null)
        {
          jsonObj.put(JsonTemplateProcessor.TEMPLATE_MAPPING_PARAM, templatesJson);
        }
        senseiReq = SenseiRequest.fromJSON(jsonObj);
      }
      SenseiResult res = _senseiBroker.browse(senseiReq);
      OutputStream ostream = resp.getOutputStream();
      convertResult(senseiReq,res,ostream);
      ostream.flush();
    }
    catch (Exception e)
    {
      throw new ServletException(e.getMessage(),e);
    }
  }

  private void handleStoreGetRequest(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    SenseiRequest senseiReq = null;
    try
    {
      JSONArray ids = null;
      if ("post".equalsIgnoreCase(req.getMethod()))
      {
        BufferedReader reader = req.getReader();
        ids = new JSONArray(readContent(reader));
      }
      else
      {
        String jsonString = req.getParameter("json");
        if (jsonString != null)
          ids = new JSONArray(jsonString);
      }

      if (queryLogger.isInfoEnabled()){
        queryLogger.info("get="+String.valueOf(ids));
      }

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

      JSONObject ret = new JSONObject();
      JSONObject obj = null;
      if (res != null && res.getSenseiHits() != null)
      {
        for (SenseiHit hit : res.getSenseiHits())
        {
          try
          {
            obj = new JSONObject(hit.getSrcData());
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
  }

  private void handleSystemInfoRequest(HttpServletRequest req, HttpServletResponse resp)
    throws ServletException, IOException {
    try {
      SenseiSystemInfo res = _senseiSysBroker.browse(new SenseiRequest());
      OutputStream ostream = resp.getOutputStream();
      convertResult(res, ostream);
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
      handleSenseiRequest(req, resp);
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
    }
    else
    {
      handleSenseiRequest(req, resp);
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

  protected abstract void convertResult(SenseiSystemInfo info, OutputStream ostream) throws Exception;

  protected abstract void convertResult(SenseiRequest req,SenseiResult res,OutputStream ostream) throws Exception;

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
            if (_clusterClient!=null){
              _clusterClient.shutdown();
              _clusterClient = null;
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
