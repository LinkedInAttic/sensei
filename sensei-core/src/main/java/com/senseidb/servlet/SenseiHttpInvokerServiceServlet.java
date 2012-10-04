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

import java.io.IOException;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.remoting.httpinvoker.HttpInvokerServiceExporter;
import org.springframework.util.StringUtils;
import org.springframework.web.HttpRequestMethodNotSupportedException;

import com.senseidb.svc.api.SenseiService;
import com.senseidb.svc.impl.ClusteredSenseiServiceImpl;

public class SenseiHttpInvokerServiceServlet extends
		ZookeeperConfigurableServlet {
	
	private static final long serialVersionUID = 1L;

	private ClusteredSenseiServiceImpl innerSvc;
	private HttpInvokerServiceExporter target;

	@Override
	public void init(ServletConfig config) throws ServletException {
		super.init(config);
		
		innerSvc = new ClusteredSenseiServiceImpl(senseiConf, loadBalancerFactory, versionComparator);
		innerSvc.start();
		target = new HttpInvokerServiceExporter();
		target.setService(innerSvc);
		target.setServiceInterface(SenseiService.class);
		target.afterPropertiesSet();
	}
	
	protected void service(HttpServletRequest request, HttpServletResponse response)
	throws ServletException, IOException {

      try {
	    this.target.handleRequest(request, response);
      }
      catch (HttpRequestMethodNotSupportedException ex) {
	    String[] supportedMethods = ((HttpRequestMethodNotSupportedException) ex).getSupportedMethods();
	    if (supportedMethods != null) {
		  response.setHeader("Allow", StringUtils.arrayToDelimitedString(supportedMethods, ", "));
	    }
	    response.sendError(HttpServletResponse.SC_METHOD_NOT_ALLOWED, ex.getMessage());
      }
    }

	@Override
	public void destroy() {
		try{
		  innerSvc.shutdown();
		}
		finally{
		  super.destroy();
		}
	}
}
