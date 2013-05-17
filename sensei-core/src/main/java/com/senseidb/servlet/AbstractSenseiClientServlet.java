/**
 * This software is licensed to you under the Apache License, Version 2.0 (the
 * "Apache License").
 *
 * LinkedIn's contributions are made under the Apache License. If you contribute
 * to the Software, the contributions will be deemed to have been made under the
 * Apache License, unless you expressly indicate otherwise. Please do not make any
 * contributions that would be inconsistent with the Apache License.
 *
 * You may obtain a copy of the Apache License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, this software
 * distributed under the Apache License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the Apache
 * License for the specific language governing permissions and limitations for the
 * software governed under the Apache License.
 *
 * © 2012 LinkedIn Corp. All Rights Reserved.  
 */
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
import com.senseidb.search.req.SenseiRequest;
import com.senseidb.search.req.SenseiResult;
import com.senseidb.search.req.SenseiSystemInfo;
import com.senseidb.svc.impl.HttpRestSenseiServiceImpl;
import com.senseidb.util.JsonTemplateProcessor;
import com.senseidb.util.RequestConverter2;

public abstract class AbstractSenseiClientServlet extends ZookeeperConfigurableServlet {

  public static final int JSON_PARSING_ERROR = 489;
  public static final int BQL_EXTRA_FILTER_ERROR = 498;
  public static final int BQL_PARSING_ERROR = 499;
  public static final String BQL_STMT = "bql";
  public static final String BQL_EXTRA_FILTER = "bql_extra_filter";
  private static final long serialVersionUID = 1L;

  private static final Logger logger = Logger.getLogger(AbstractSenseiClientServlet.class);
  private static final Logger queryLogger = Logger.getLogger("com.sensei.querylog");

  private final NetworkClientConfig _networkClientConfig = new NetworkClientConfig();

  private ClusterClient _clusterClient = null;
  private SenseiNetworkClient _networkClient = null;
  private SenseiBroker _senseiBroker = null;
  private SenseiSysBroker _senseiSysBroker = null;
  private Map<String, String[]> _facetInfoMap = new HashMap<String, String[]>();
  private BQLCompiler _compiler = null;
  private LayeredBroker federatedBroker;

  public AbstractSenseiClientServlet() {

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
    
    SenseiBrokerExport export = (SenseiBrokerExport)config.getServletContext().getAttribute("sensei.broker.export");
    export.broker = _senseiBroker;
    export.sysBroker = _senseiSysBroker;
    export.networkClient = _networkClient;
    export.clusterClient = _clusterClient;
    export.servlet = this;

    int count = 0;
    while (true)
    {
      try
      {
        count++;
        logger.info("Trying to get sysinfo");
        SenseiSystemInfo sysInfo = _senseiSysBroker.browse(new SenseiRequest());

        _facetInfoMap = extractFacetInfo(sysInfo);
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
    export.facetInfo = _facetInfoMap;
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

  private void handleSenseiRequest(HttpServletRequest req, HttpServletResponse resp, Broker<SenseiRequest, SenseiResult> broker)
      throws ServletException, IOException {
    long time = System.currentTimeMillis();
    int numHits = 0, totalDocs = 0;
    String query = null;

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
              writeEmptyResponse(req, resp, new SenseiError(jse.getMessage(), ErrorType.JsonParsingError));              
              return;            
          }

          logger.warn("Old client or json error", jse);

          // Fall back to the old REST API.  In the future, we should
          // consider reporting JSON exceptions here.
          senseiReq = DefaultSenseiJSONServlet.convertSenseiRequest(
                        new DataConfiguration(new MapConfiguration(getParameters(content))));
          query = content;
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
            writeEmptyResponse(req, resp, new SenseiError(jse.getMessage(), ErrorType.JsonParsingError));
            return;
          }
        }
        else
        {
          senseiReq = buildSenseiRequest(req);
          query = URLEncodedUtils.format(
                    HttpRestSenseiServiceImpl.convertRequestToQueryParams(senseiReq), "UTF-8");
        }
      }

      if (jsonObj != null)
      {
        String bqlStmt = jsonObj.optString(BQL_STMT);
        JSONObject templatesJson = jsonObj.optJSONObject(JsonTemplateProcessor.TEMPLATE_MAPPING_PARAM);
        JSONObject compiledJson = null;

        if (bqlStmt.length() > 0)
        {
          try
          {
            if (jsonObj.length() == 1)
              query = "bql=" + bqlStmt;
            else
              query = "json=" + content;
            compiledJson = _compiler.compile(bqlStmt);
          }
          catch (RecognitionException e)
          {
            String errMsg = _compiler.getErrorMessage(e);
            if (errMsg == null) 
            {
              errMsg = "Unknown parsing error.";
            }
            logger.error("BQL parsing error: " + errMsg + ", BQL: " + bqlStmt);
            writeEmptyResponse(req, resp, new SenseiError(errMsg, ErrorType.BQLParsingError));
            return;
          }

          // Handle extra BQL filter if it exists
          String extraFilter = jsonObj.optString(BQL_EXTRA_FILTER);
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
              return;
            }

            // Combine filters
            JSONArray filter_list = new JSONArray();
            JSONObject currentFilter = compiledJson.optJSONObject("filter");
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
              compiledJson.put("filter", new JSONObject().put("and", filter_list));
            }
            else if (filter_list.length() == 1)
            {
              compiledJson.put("filter", filter_list.get(0));
            }
          }

          JSONObject metaData = compiledJson.optJSONObject("meta");
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
                  writeEmptyResponse(req, resp, new SenseiError("[line:0, col:0] Variable " + var + " is not found.", ErrorType.BQLParsingError));
                  return;
                }
              }
            }
          }
        }
        else
        {
          // This is NOT a BQL statement
          query = "json=" + content;
          compiledJson = jsonObj;
        }

        if (templatesJson != null)
        {
          compiledJson.put(JsonTemplateProcessor.TEMPLATE_MAPPING_PARAM, templatesJson);
        }
        senseiReq = SenseiRequest.fromJSON(compiledJson, _facetInfoMap);
      }
      SenseiResult res = broker.browse(senseiReq);
      numHits = res.getNumHits();
      totalDocs = res.getTotalDocs();
      sendResponse(req, resp, senseiReq, res);
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
      if (queryLogger.isInfoEnabled() && query != null)
      {
        queryLogger.info(String.format("hits(%d/%d) took %dms: %s", numHits, totalDocs, System.currentTimeMillis() - time, query));
      }
    }
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
        ids = new JSONArray(readContent(reader));
      }
      else
      {
        String jsonString = req.getParameter("json");
        if (jsonString != null)
          ids = new JSONArray(jsonString);
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

  /**
   * This class is a hack to allow the servlet to export the broker components
   * to outside services. Since this components are instantiated here as part 
   * of the servlet initialization, it is a workaround to expose it through a 
   * placed holder instance injected via ServletContext by the builder of this
   * servlet and its container. 
   */
  public static class SenseiBrokerExport {
    public AbstractSenseiClientServlet servlet;
    public ClusterClient clusterClient;
    public SenseiNetworkClient networkClient;
    public SenseiSysBroker sysBroker;
    public SenseiBroker broker;
    public Map<String, String[]> facetInfo;
  }
}
