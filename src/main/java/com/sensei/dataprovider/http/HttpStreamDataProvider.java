package com.sensei.dataprovider.http;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.SimpleHttpConnectionManager;
import org.apache.commons.httpclient.contrib.ssl.EasySSLProtocolSocketFactory;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.protocol.Protocol;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;

import proj.zoie.api.ZoieVersion;
import proj.zoie.api.DataConsumer.DataEvent;
import proj.zoie.impl.indexing.StreamDataProvider;

import com.sensei.search.req.SenseiQuery;

public class HttpStreamDataProvider<D,V extends ZoieVersion> extends StreamDataProvider<D, V> {

	private static final Logger logger = Logger.getLogger(HttpStreamDataProvider.class);
	
	private final String _baseUrl;

	private SimpleHttpConnectionManager _httpClientManager;
	private HttpClient _httpClient;
	  
	public static final int DEFAULT_TIMEOUT_MS = 10000;
	
	public static final String DEFAULT_OFFSET_PARAM = "offset";
	public static final String DFEAULT_DATA_PARAM = "data";
	
	private final int _batchSize;
	private final String _password;
	private String _offset;
	private String _initialOffset;
	private final boolean _disableHttps;
	
	private String _offsetParam;
	private String _dataParam;
	
	private List<JSONObject> _currentList = null;
	
	public HttpStreamDataProvider(String baseUrl,String pw,int batchSize,String startingOffset,boolean disableHttps){
	  _baseUrl = baseUrl;
	  _password = pw;
	  _batchSize = batchSize;
	  _offset = startingOffset;
	  _disableHttps = disableHttps;
	  _initialOffset = null;
	  if (!_disableHttps){
	    Protocol.registerProtocol("https", 
                new Protocol("https", new EasySSLProtocolSocketFactory(), 443));
	  }
	  _offsetParam = DEFAULT_OFFSET_PARAM;
	  _dataParam = DFEAULT_DATA_PARAM;
	}
	
	public void setInitialOffset(String initialOffset){
	  _initialOffset = initialOffset;
	}
	
	private String buildGetString(String offset){
		StringBuilder buf = new StringBuilder();
		buf.append(_baseUrl);
		buf.append("?password=").append(_password);
		buf.append("&num=").append(_batchSize);
		String sinceKey;
		if (_offset != null && ! "null".equalsIgnoreCase(_offset) && ! "now".equalsIgnoreCase(_offset)){
			sinceKey=offset;
        }
		else{
			_offset=null;
			sinceKey=null;
		}
		
		if (sinceKey!=null){
			buf.append("&since=").append(sinceKey);
		}
		return buf.toString();
	}
	
	private static class FetchedData{
		String offset;
		List<JSONObject> dataList;
	}
	
	private FetchedData parse(Reader reader) throws JSONException{
      JSONObject json = new JSONObject(new JSONTokener(reader));
  
      String offset = json.getString(_offsetParam);
      JSONArray data = json.getJSONArray(_dataParam);
      
      FetchedData fetchedData = new FetchedData();
      fetchedData.offset = offset;
      
      ArrayList<JSONObject> arrayList = new ArrayList<JSONObject>(data.length());
      fetchedData.dataList = arrayList;
      
      for (int i=0;i<data.length();++i){
    	  arrayList.add(data.optJSONObject(i));
      }
      return fetchedData;
    }
	
	private FetchedData fetchBatch() throws IOException{
	  GetMethod getMethod = null; 
	  BufferedReader reader = null;
	  try{
	    getMethod = new GetMethod(buildGetString(_offset));
	    int statusCode = _httpClient.executeMethod(getMethod);
        
        if (statusCode != HttpStatus.SC_OK){
          throw new IOException("invalid status: "+statusCode);
        }
        
        InputStream stream = getMethod.getResponseBodyAsStream();
        reader = new BufferedReader(new InputStreamReader(stream,SenseiQuery.utf8Charset));
        return parse(reader);
	  }
	  catch(JSONException jse){
		throw new IOException(jse.getMessage(),jse);
	  }
	  finally{
		if (reader != null){
          try{
            reader.close();
          }
          catch (Exception e){
            logger.error(e.getMessage(), e);
          }
        }
        if (getMethod != null){
          try{
            getMethod.releaseConnection();
          }
          catch (Exception e){
            logger.error(e.getMessage(), e);
          }
        }
	  }
	}
	
	@Override
	public DataEvent<D, V> next() {
	  if (_currentList==null || _currentList.size() == 0){
		 // FetchedData data = fetchBatch();
		  
	  }
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void reset() {
	  if (_initialOffset!=null){
		  _offset = _initialOffset;
	  }
	}

	@Override
	public void start() {
		super.start();
		_httpClientManager = new SimpleHttpConnectionManager();
	    _httpClientManager.getParams().setConnectionTimeout(DEFAULT_TIMEOUT_MS);
	    _httpClientManager.getParams().setSoTimeout(DEFAULT_TIMEOUT_MS);
	    _httpClient = new HttpClient(_httpClientManager);
	}

	@Override
	public void stop() {
		try{
		  if (_httpClientManager!=null){
			_httpClient = null;
		    _httpClientManager.shutdown();
		    _httpClientManager = null;
		  }
		}
		finally{
		  super.stop();
		}
	}
}
