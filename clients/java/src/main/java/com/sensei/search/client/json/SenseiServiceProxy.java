package com.sensei.search.client.json;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.tsccm.ThreadSafeClientConnManager;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.CoreConnectionPNames;
import org.apache.http.params.CoreProtocolPNames;
import org.apache.http.params.HttpParams;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.sensei.search.client.json.req.SenseiClientRequest;
import com.sensei.search.client.json.res.SenseiResult;

public class SenseiServiceProxy {
    private static Log LOG = LogFactory.getLog(SenseiServiceProxy.class);
    private  String host;
    private  int port;
    private static HttpClient httpclient;

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
    }

   public SenseiServiceProxy(String host, int port) {
      this.host = host;
      this.port = port;
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
    public List<Map<String, Object>> sendGetRequest(List<String> uids) throws IOException, JSONException {
      List<Map<String, Object>> ret = new ArrayList<Map<String, Object>>(uids.size());
      String response = sendPostRaw(getStoreGetUrl(), new JSONArray(uids).toString());
      if (response == null || response.length() == 0) {
        return ret;
      }
      JSONArray jsonArray = new JSONArray(response);
      for (int i = 0; i < jsonArray.length(); i++) {
        Map<String, Object> item = new HashMap<String, Object>();
        JSONObject jsonItem = jsonArray.optJSONObject(i);
        if (jsonItem == null) {
          continue;
        }
        Iterator keys = jsonItem.keys();
        while (keys.hasNext()) {
          String key = (String) keys.next();
          item.put(key, jsonItem.opt(key));
        }
        ret.add(item);

      }
      return ret;
    }
    public String getSearchUrl() {
      return "http://" + host + ":" + port + "/sensei";
    }
    public String getStoreGetUrl() {
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

    public void close() {
      getHttpClient().getConnectionManager().shutdown();
    }
    public String sendPostRaw(String urlStr, String requestStr){
        HttpURLConnection conn = null;
          try {
          if (LOG.isInfoEnabled()){
            LOG.info("Sending a post request to the server - " + urlStr);
          }

          if (LOG.isDebugEnabled()){
            LOG.debug("The request is - " + requestStr);
          }
          URL url = new URL(urlStr);
           conn = (HttpURLConnection) url.openConnection();
          conn.setDoOutput(true);
          conn.setRequestMethod("POST");
          conn.setRequestProperty("Content-Type", "application/json; charset=utf-8");

          conn.setRequestProperty("Accept-Encoding", "gzip");

         String string = requestStr;
          byte[] requestBytes = string.getBytes("UTF-8");
          conn.setRequestProperty("Content-Length", String.valueOf(requestBytes.length));
          conn.setRequestProperty("http.keepAlive", String.valueOf(true));
          conn.setRequestProperty("default", String.valueOf(true));

          //GZIPOutputStream zippedOutputStream = new GZIPOutputStream(conn.getOutputStream());
          OutputStream os = new BufferedOutputStream( conn.getOutputStream());
          os.write(requestBytes);
          os.flush();
          os.close();
          int responseCode = conn.getResponseCode();

          if (LOG.isInfoEnabled()){
            LOG.info("The http response code is " + responseCode);
          }
          if (responseCode != HttpURLConnection.HTTP_OK) {
              throw new IOException("Failed : HTTP error code : "
                  + responseCode);
          }
          byte[] bytes = drain(new GZIPInputStream(new BufferedInputStream( conn.getInputStream())));

          String output = new String(bytes, "UTF-8");
          if (LOG.isDebugEnabled()){
            LOG.debug("The response from the server is - " + output);
          }
          return output;
          } catch (Exception ex) {
            throw new RuntimeException (ex);
          }
           finally {
            if (conn != null) conn.disconnect();
          }
    }

}
