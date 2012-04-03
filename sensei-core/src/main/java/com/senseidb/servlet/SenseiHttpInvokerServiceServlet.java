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
		
		innerSvc = new ClusteredSenseiServiceImpl(zkurl, zkTimeout, clusterClientName, clusterName, connectTimeoutMillis,
                                              writeTimeoutMillis, maxConnectionsPerNode, staleRequestTimeoutMins,
                                              staleRequestCleanupFrequencyMins,
                                              loadBalancerFactory, versionComparator,
                                              pollInterval, minResponses, maxTotalWait);
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
