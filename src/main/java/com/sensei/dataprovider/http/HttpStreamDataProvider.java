package com.sensei.dataprovider.http;

import java.io.IOException;
import java.io.InputStream;
import java.util.Comparator;
import java.util.Iterator;

import org.apache.commons.io.IOUtils;
import org.apache.http.Header;
import org.apache.http.HeaderElement;
import org.apache.http.HttpEntity;
import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.HttpResponse;
import org.apache.http.HttpResponseInterceptor;
import org.apache.http.HttpVersion;
import org.apache.http.StatusLine;
import org.apache.http.client.entity.GzipDecompressingEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.conn.scheme.PlainSocketFactory;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.SingleClientConnManager;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.params.HttpParams;
import org.apache.http.params.HttpProtocolParams;
import org.apache.http.protocol.HttpContext;
import org.apache.log4j.Logger;

import proj.zoie.api.DataConsumer.DataEvent;
import proj.zoie.impl.indexing.StreamDataProvider;

public abstract class HttpStreamDataProvider<D> extends StreamDataProvider<D,StringZoieVersion> {

	private static final Logger logger = Logger.getLogger(HttpStreamDataProvider.class);
	
	protected final String _baseUrl;

	private final ClientConnectionManager _httpClientManager;
	private final DefaultHttpClient _httpclient;
	  
	public static final int DEFAULT_TIMEOUT_MS = 10000;
	
	public static final String DEFAULT_OFFSET_PARAM = "offset";
	public static final String DFEAULT_DATA_PARAM = "data";
	
	protected final int _batchSize;
	protected final String _password;
	protected String _offset;
	protected String _initialOffset;
	private final boolean _disableHttps;
	private  Iterator<DataEvent<D,StringZoieVersion>> _currentDataIter;
	protected Comparator<String> _versionComparator;
	private volatile boolean _stopped;
	private int _retryTime;
	
	public HttpStreamDataProvider(String baseUrl,String pw,int batchSize,String startingOffset,boolean disableHttps){
	  _baseUrl = baseUrl;
	  _password = pw;
	  _batchSize = batchSize;
	  _offset = startingOffset;
	  _disableHttps = disableHttps;
	  _initialOffset = null;
	  _currentDataIter = null;
	  _stopped = true;

	  Scheme http = new Scheme("http", 80, PlainSocketFactory.getSocketFactory());
	  SchemeRegistry sr = new SchemeRegistry();
	  sr.register(http);
	  
	  if (!_disableHttps){
		  Scheme https = new Scheme("https",443,SSLSocketFactory.getSocketFactory());
		  sr.register(https);
	  }
	  
	  HttpParams params = new BasicHttpParams();
	  params.setParameter(HttpProtocolParams.PROTOCOL_VERSION,
		      HttpVersion.HTTP_1_1);
	  params.setParameter(HttpProtocolParams.HTTP_CONTENT_CHARSET, "UTF-8");
	  params.setIntParameter(HttpConnectionParams.CONNECTION_TIMEOUT,5000);   // 5s conn timeout
	  params.setIntParameter(HttpConnectionParams.SO_LINGER, 0);  //  no socket linger
	  params.setBooleanParameter(HttpConnectionParams.TCP_NODELAY, true); // tcp no delay
	  params.setIntParameter(HttpConnectionParams.SO_TIMEOUT,5000);  // 5s sock timeout
	  params.setIntParameter(HttpConnectionParams.SOCKET_BUFFER_SIZE,1024*1024);  // 1mb socket buffer
	  params.setBooleanParameter(HttpConnectionParams.SO_REUSEADDR,true);  // 5s sock timeout
	  
	  _httpClientManager = new SingleClientConnManager(sr);
	  _httpclient = new DefaultHttpClient(_httpClientManager,params);
	  
	  _httpclient.addRequestInterceptor(new HttpRequestInterceptor() {
	      public void process(final HttpRequest request, final HttpContext context)
	        throws HttpException, IOException {
	        if (!request.containsHeader("Accept-Encoding")) {
	          request.addHeader("Accept-Encoding", "gzip");
	        }
	      }
	    });

	  _httpclient.addResponseInterceptor(new HttpResponseInterceptor() {
	      public void process(final HttpResponse response, final HttpContext context)
	        throws HttpException, IOException {
	        HttpEntity entity = response.getEntity();
	        Header ceheader = entity.getContentEncoding();
	        if (ceheader != null) {
	          HeaderElement[] codecs = ceheader.getElements();
	          for (int i = 0; i < codecs.length; i++) {
	            if (codecs[i].getName().equalsIgnoreCase("gzip")) {
	            	System.out.println("gzip response");
	              response.setEntity(new GzipDecompressingEntity(response
	                .getEntity()));
	              return;
	            }
	          }
	        }
	      }
	    });
	  
	  _versionComparator = getVersionComparator();
	  _retryTime = 5000;   // default retry after 5 seconds
	}
	
	public void setRetryTime(int retryTime){
		_retryTime = retryTime;
	}
	
	public int getRetryTime(){
		return _retryTime;
	}
	
	public void setInitialOffset(String initialOffset){
	  _initialOffset = initialOffset;
	}
	
	protected abstract String buildGetString(String offset);
	
	protected abstract Comparator<String> getVersionComparator();
	
	protected abstract  Iterator<DataEvent<D,StringZoieVersion>> parse(InputStream is) throws Exception;

	private Iterator<DataEvent<D,StringZoieVersion>> fetchBatch() throws HttpException{
	  InputStream stream = null;
	  try{
		HttpGet httpget = new HttpGet(buildGetString(_offset));
	    HttpResponse response = _httpclient.execute(httpget);
	    HttpEntity entity = response.getEntity();
	    StatusLine status = response.getStatusLine();
	    int statusCode = status.getStatusCode();
	    
        if (statusCode >= 400){
          try {
            IOUtils.closeQuietly(entity.getContent());
          }
          catch (Exception e) {
        	logger.error(e.getMessage(),e);
          }
          throw new HttpException(status.getReasonPhrase());
        }
        
        try{
          stream = entity.getContent();
          return parse(stream);
        }
        catch(Exception e){
          logger.error(e.getMessage(),e);
          httpget.abort();
          throw new HttpException(e.getMessage(),e);
        }
	  }
	  catch(IOException ioe){
		throw new HttpException(ioe.getMessage(),ioe);
	  }
	  finally{
		if (stream != null){
          IOUtils.closeQuietly(stream);
        }
	  }
	}
	
	@Override
	public DataEvent<D,StringZoieVersion> next() {
	  if (_stopped){
		  return null;
	  }
	  if (_currentDataIter==null || !_currentDataIter.hasNext()){
		while(true && !_stopped){
		  try{
		    Iterator<DataEvent<D,StringZoieVersion>> data = fetchBatch();
		   
		    if (data==null || !data.hasNext()){
		      if (logger.isDebugEnabled()){
			    logger.debug("no more data");
		      }
		      return null;
		    }
		    _currentDataIter = data;
		    break;
		  } catch (HttpException e) {
		    logger.error(e.getMessage(),e);
		    try {
		    	logger.error("retrying in "+_retryTime+"ms");
				Thread.sleep(_retryTime);
				continue;
			} catch (InterruptedException e1) {
				return null;
			}
		  }  
		}
	  }
	  
	  DataEvent<D,StringZoieVersion> data = _currentDataIter.next();
	  _offset = data.getVersion().encodeToString();
	  return data;
	  
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
		_stopped=false;
	}

	@Override
	public void stop() {
		try{
		  super.stop();
		}
		finally{
		  _stopped = true;
		  if (_httpClientManager!=null){
			 _httpClientManager.shutdown();
		   }
		}
	}
}
