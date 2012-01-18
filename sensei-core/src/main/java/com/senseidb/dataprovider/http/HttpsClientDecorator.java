package com.senseidb.dataprovider.http;

import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.log4j.Logger;

public class HttpsClientDecorator {
	private static Logger logger = Logger.getLogger(HttpsClientDecorator.class);
	
	public static DefaultHttpClient decorate(DefaultHttpClient base) {
	    try {
	      SSLContext ctx = SSLContext.getInstance("TLS");
	      X509TrustManager tm = new X509TrustManager() {
	 
	        public void checkClientTrusted(X509Certificate[] xcs, String string) throws CertificateException {
	        }
	 
	        public void checkServerTrusted(X509Certificate[] xcs, String string) throws CertificateException {
	        }
	 
	        public X509Certificate[] getAcceptedIssuers() {
	          return null;
	        }
	      };
	      ctx.init(null, new TrustManager[]{tm}, null);
	      SSLSocketFactory ssf = new SSLSocketFactory(ctx,SSLSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER);
	      ClientConnectionManager ccm = base.getConnectionManager();
	      SchemeRegistry sr = ccm.getSchemeRegistry();
	      sr.register(new Scheme("https",443,ssf));
	      return new DefaultHttpClient(ccm, base.getParams());
	    } catch (Exception ex) {
	      logger.error(ex.getMessage(),ex);
	      return null;
	    }
	  }
}
