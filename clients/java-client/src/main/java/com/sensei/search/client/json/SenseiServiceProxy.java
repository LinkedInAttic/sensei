package com.sensei.search.client.json;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.tsccm.ThreadSafeClientConnManager;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.sensei.search.client.json.req.SenseiClientRequest;
import com.sensei.search.client.json.res.SenseiResult;

public class SenseiServiceProxy {
    private static Log LOG = LogFactory.getLog(SenseiServiceProxy.class);
    private  String host;
    private  String port;
    private static HttpClient httpclient;

    private static synchronized HttpClient getHttpClient() {
      if (httpclient == null) {
        ThreadSafeClientConnManager cm = new ThreadSafeClientConnManager();
        cm.setMaxTotal(100);
        httpclient = new DefaultHttpClient(cm);
      }
      return httpclient;
    }

   public SenseiServiceProxy(String host, String port) {
      super();
      this.host = host;
      this.port = port;
    }

    public SenseiResult sendSearchRequest( SenseiClientRequest request) throws IOException, JSONException {
    	String requestStr = JsonSerializer.serialize(request).toString();
        String output = sendPost(getSearchUrl(), requestStr);
        //System.out.println("Output from Server = " + output);
        return JsonDeserializer.deserialize(SenseiResult.class, jsonResponse(output));
    }
    public List<Map<String, Object>> sendGetRequest(List<String> uids) throws IOException, JSONException {
      List<Map<String, Object>> ret = new ArrayList<Map<String, Object>>(uids.size());
      String response = sendPost(getStoreGetUrl(), new JSONArray(uids).toString());
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
	  public String sendPost(String urlStr, String requestStr) {
			HttpPost httpPost = new HttpPost(urlStr);
			try {
			httpPost.setHeader("Content-Type", "application/json; charset=utf-8");
			httpPost.setHeader("Accept-Encoding", "gzip");
			httpPost.setEntity(new StringEntity(requestStr));
			 if (LOG.isInfoEnabled()){
         LOG.info("Sending a post request to the server - " + urlStr);
       }

       if (LOG.isDebugEnabled()){
         LOG.debug("The request is - " + requestStr);
       }
			HttpResponse response = getHttpClient().execute(httpPost);
			 int responseCode = response.getStatusLine().getStatusCode();

       if (LOG.isInfoEnabled()){
         LOG.info("The http response code is " + responseCode);
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
	}
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
    /*public String sendPost(String urlStr, String requestStr)
        throws MalformedURLException, IOException, ProtocolException,
        UnsupportedEncodingException {
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
          } finally {
            if (conn != null) conn.disconnect();
          }
    }
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
      }*/
}
