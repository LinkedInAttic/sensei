package com.senseidb.search.client;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.zip.GZIPInputStream;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.senseidb.search.client.json.JsonDeserializer;
import com.senseidb.search.client.json.JsonSerializer;
import com.senseidb.search.client.req.SenseiClientRequest;
import com.senseidb.search.client.res.SenseiResult;
public class SenseiServiceProxy {
 // private static Logger LOG = LoggerFactory.getLogger(SenseiServiceProxy.class);


    private  String host;
    private  int port;
    private final String url;
    /*private static HttpClient httpclient;

    private static synchronized HttpClient getHttpClient() {
      if (httpclient == null) {
        ThreadSafeClientConnManager cm = new ThreadSafeClientConnManager();
        cm.setMaxTotal(200);
        cm.setDefaultMaxPerRoute(100);
        HttpParams params = new BasicHttpParams();
        params.setParameter(CoreConnectionPNames.CONNECTION_TIMEOUT, 100);
        params.setParameter(CoreConnectionPNames.TCP_NODELAY, false);
        params.setParameter(CoreConnectionPNames.STALE_CONNECTION_CHECK, false);
        params.setParameter(CoreProtocolPNames.USE_EXPECT_CONTINUE, Boolean.FALSE);
        //params.setParameter("http.protocol.version", HttpVersion.HTTP_1_1);
        httpclient = new DefaultHttpClient(cm, params);

      }
      return httpclient;
    }*/

   public SenseiServiceProxy(String host, int port) {
      this.host = host;
      this.port = port;
      this.url = null;
    }
   public SenseiServiceProxy(String url) {
    this.url = url;
    
   }
    public SenseiResult sendSearchRequest( SenseiClientRequest request)  {
      try {
      String requestStr = JsonSerializer.serialize(request).toString();
        String output = sendPostRaw(getSearchUrl(), requestStr);
        //System.out.println("Output from Server = " + output);
        return JsonDeserializer.deserialize(SenseiResult.class, jsonResponse(output));
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    }
    public SenseiResult sendBQL( String bql) {
      try {
        StringBuilder buffer = new StringBuilder();
        buffer.append("{'bql':").append(bql).append("}");
        String requestStr = buffer.toString();
        String output = sendPostRaw(getSearchUrl(), requestStr);
        return JsonDeserializer.deserialize(SenseiResult.class, jsonResponse(output));
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    }
    

  public Map<Long, JSONObject> sendGetRequest(long... uids) throws IOException, JSONException {
    Map<Long, JSONObject> ret = new LinkedHashMap<Long, JSONObject>(uids.length);
    String response = sendPostRaw(getStoreGetUrl(), new JSONArray(uids).toString());
    if (response == null || response.length() == 0) {
      return ret;
    }
    JSONObject responseJson = new JSONObject(response);

    Iterator keys = responseJson.keys();
    while (keys.hasNext()) {
      String key = (String) keys.next();
      ret.put(Long.parseLong(key), responseJson.optJSONObject(key));
    }

    return ret;
  }
    public String getSearchUrl() {
      if (url != null) return url;
      return "http://" + host + ":" + port + "/sensei";
    }
    public String getStoreGetUrl() {
      if (url != null) return url + "/get";
      return "http://" + host + ":" + port + "/sensei/get";
    }
    /*public String sendPost(String path, String requestStr) {
      HttpPost httpPost = new HttpPost(path);
      try {
      httpPost.setHeader("Content-Type", "application/json; charset=utf-8");
      httpPost.setHeader("Accept-Encoding", "gzip");
      httpPost.setHeader("http.keepAlive", String.valueOf(true));
      httpPost.setHeader("default", String.valueOf(true));
      httpPost.setEntity(new StringEntity(requestStr));
       if (LOG.isDebugEnabled()){
         LOG.debug("Sending a post request to the server - " + path);
       }

       if (LOG.isDebugEnabled()){
         LOG.debug("The request is - " + requestStr);
       }
      HttpResponse response = getHttpClient().execute(httpPost);
       int responseCode = response.getStatusLine().getStatusCode();

       if (LOG.isDebugEnabled()){
         LOG.debug("The http response code is " + responseCode);
       }
       if (responseCode != HttpURLConnection.HTTP_OK) {
           throw new IOException("Failed : HTTP error code : "
               + responseCode);
       }
       HttpEntity entity = response.getEntity();

       byte[] bytes = drain(new GZIPInputStream(new BufferedInputStream( entity.getContent())));

       String output = new String(bytes, "UTF-8");
       if (LOG.isDebugEnabled()){
         LOG.debug("The response from the server is - " + output);
       }
       return output;
      } catch (Exception ex) {
        httpPost.abort();
        throw new RuntimeException(ex);
      }
  }*/
    private JSONObject jsonResponse(String output) throws JSONException {
        return new JSONObject(output);
    }
    byte[] drain(InputStream inputStream) throws IOException {
        try {
        byte[] buf = new byte[1024];
        int len;
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                while ((len = inputStream.read(buf)) > 0) {
                    byteArrayOutputStream.write(buf, 0, len);
                }
        return byteArrayOutputStream.toByteArray();
        } finally {
            inputStream.close();
        }
    }


    public String sendPostRaw(String urlStr, String requestStr){
      return this.sendPostRaw(urlStr, requestStr,null);
    }
    
    /*public void close() {
      getHttpClient().getConnectionManager().shutdown();
    }*/
    public String sendPostRaw(String urlStr, String requestStr,Map<String,String> headers){
        HttpURLConnection conn = null;
          try {
          /*if (LOG.isInfoEnabled()){
            LOG.info("Sending a post request to the server - " + urlStr);
          }

          if (LOG.isDebugEnabled()){
            LOG.debug("The request is - " + requestStr);
          }*/
         
           URL url = new URL(urlStr);
           conn = (HttpURLConnection) url.openConnection();
          conn.setDoOutput(true);
          conn.setRequestMethod("POST");
         // conn.setRequestProperty("Content-Type", "application/json; charset=utf-8");

          conn.setRequestProperty("Accept-Encoding", "gzip");

         String string = requestStr;
          byte[] requestBytes = string.getBytes("UTF-8");
          conn.setRequestProperty("Content-Length", String.valueOf(requestBytes.length));
          conn.setRequestProperty("http.keepAlive", String.valueOf(true));
          conn.setRequestProperty("default", String.valueOf(true));
          
          if (headers!=null && headers.size()>0){
            Set<Entry<String,String>> entries = headers.entrySet();
            for (Entry<String,String> entry : entries){
              conn.setRequestProperty(entry.getKey(),entry.getValue());
            }
          }
          

          //GZIPOutputStream zippedOutputStream = new GZIPOutputStream(conn.getOutputStream());
          OutputStream os = new BufferedOutputStream( conn.getOutputStream());
          os.write(requestBytes);
          os.flush();
          os.close();
          int responseCode = conn.getResponseCode();

          /*if (LOG.isInfoEnabled()){
            LOG.info("The http response code is " + responseCode);
          }*/
          if (responseCode != HttpURLConnection.HTTP_OK) {
              throw new IOException("Failed : HTTP error code : "
                  + responseCode);
          }
          byte[] bytes = drain(new GZIPInputStream(new BufferedInputStream( conn.getInputStream())));

          String output = new String(bytes, "UTF-8");
          /*if (LOG.isDebugEnabled()){
            LOG.debug("The response from the server is - " + output);
          }*/
          return output;
          } catch (Exception ex) {
            throw new RuntimeException (ex);
          }
           finally {
            if (conn != null) conn.disconnect();
          }
    }

}
