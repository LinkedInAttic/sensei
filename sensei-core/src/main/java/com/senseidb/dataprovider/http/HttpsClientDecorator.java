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
