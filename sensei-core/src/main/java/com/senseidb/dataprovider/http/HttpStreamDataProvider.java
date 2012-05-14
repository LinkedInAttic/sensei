package com.senseidb.dataprovider.http;

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

import com.linkedin.zoie.api.DataConsumer.DataEvent;
import com.linkedin.zoie.impl.indexing.StreamDataProvider;

public abstract class HttpStreamDataProvider<D> extends StreamDataProvider<D> implements HttpDataProviderAdminMBean{

	private static final Logger logger = Logger.getLogger(HttpStreamDataProvider.class);
	
	protected final String _baseUrl;

	private final ClientConnectionManager _httpClientManager;
	private DefaultHttpClient _httpclient;
	  
	public static final int DEFAULT_TIMEOUT_MS = 10000;
	public static final int DEFAULT_RETRYTIME_MS = 5000;
	
	public static final String DEFAULT_OFFSET_PARAM = "offset";
	public static final String DFEAULT_DATA_PARAM = "data";
	
	protected final int _fetchSize;
	protected final String _password;
	protected String _offset;
	protected String _initialOffset;
	private final boolean _disableHttps;
	private  Iterator<DataEvent<D>> _currentDataIter;
	private volatile boolean _stopped;
	private int _retryTime;
	
	private volatile long _httpGetLatency;
	private volatile long _responseParseLatency;
	
	
	public HttpStreamDataProvider(Comparator<String> versionComparator, String baseUrl,String pw,int fetchSize,String startingOffset,boolean disableHttps){
    super(versionComparator);
	  _baseUrl = baseUrl;
	  _password = pw;
	  _fetchSize = fetchSize;
	  _offset = startingOffset;
	  _disableHttps = disableHttps;
	  _initialOffset = null;
	  _currentDataIter = null;
	  _stopped = true;
	  
	  _httpGetLatency = 0L;
	  _responseParseLatency = 0L;

	  Scheme http = new Scheme("http", 80, PlainSocketFactory.getSocketFactory());
	  SchemeRegistry sr = new SchemeRegistry();
	  sr.register(http);
	  
	  
	  
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
	  
	  if (!_disableHttps){
		  _httpclient = HttpsClientDecorator.decorate(_httpclient);
	  }
	  
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
	              response.setEntity(new GzipDecompressingEntity(response
	                .getEntity()));
	              return;
	            }
	          }
	        }
	      }
	    });
	  
	  _retryTime = DEFAULT_RETRYTIME_MS;   // default retry after 5 seconds
	}
	
	public void setRetryTime(int retryTime){
		_retryTime = retryTime;
	}
	
	public int getRetryTime(){
		return _retryTime;
	}
	
	@Override
	public void setStartingOffset(String initialOffset){
	  _initialOffset = initialOffset;
	}
	
	protected abstract String buildGetString(String offset);
	
	protected abstract  Iterator<DataEvent<D>> parse(InputStream is) throws Exception;

	private Iterator<DataEvent<D>> fetchBatch() throws HttpException{
	  InputStream stream = null;
	  try{
		HttpGet httpget = new HttpGet(buildGetString(_offset));
		long getStart = System.currentTimeMillis();
	    HttpResponse response = _httpclient.execute(httpget);
	    long getEnd = System.currentTimeMillis();
	    _httpGetLatency = getEnd-getStart;
	    
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
          
          long parseStart = System.currentTimeMillis();
          Iterator<DataEvent<D>> iter =  parse(stream);
          long parseEnd = System.currentTimeMillis();
          _responseParseLatency = parseEnd - parseStart;
          return iter;
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
	public DataEvent<D> next() {
	  if (_stopped){
		  return null;
	  }
	  if (_currentDataIter==null || !_currentDataIter.hasNext()){
		while(true && !_stopped){
		  try{
		    Iterator<DataEvent<D>> data = fetchBatch();
		   
		    if (data==null || !data.hasNext()){
		      if (logger.isDebugEnabled()){
			    logger.debug("no more data");
		      }
		      synchronized(this){
		    	  try{
		            this.wait(_retryTime);
		            return null;
				  }
				  catch (InterruptedException e1) {
					return null;
				  }
		      }
		      
		    }
		    _currentDataIter = data;
		    break;
		  } catch (HttpException e) {
		    logger.error(e.getMessage(),e);
		    try {
		    	logger.error("retrying in "+_retryTime+"ms");
		    	synchronized(this){
				  this.wait(_retryTime);
		    	}
				continue;
			} catch (InterruptedException e1) {
				return null;
			}
		  }  
		}
	  }
	  
	  DataEvent<D> data = null;
	  if (_currentDataIter.hasNext()){
	    data = _currentDataIter.next();
	    if (data!=null){
	      _offset = data.getVersion();
	    }
	  }
	  return data;
	}

	@Override
	public void reset() {
	  if (_initialOffset!=null){
		  _offset = _initialOffset;
	  }
	}
	
	

	@Override
	public long getHttpGetLatency() {
		return _httpGetLatency;
	}

	@Override
	public long getResponseParseLatency() {
		return _responseParseLatency;
	}

	@Override
	public void start() {
		super.start();
		_stopped=false;
	}

	@Override
	public void stop() {
		synchronized(this){
		  _stopped = true;
		  this.notifyAll();
		}
		try{
		  super.stop();
		}
		finally{
		  
		  if (_httpClientManager!=null){
			 _httpClientManager.shutdown();
		   }
		}
	}
}
