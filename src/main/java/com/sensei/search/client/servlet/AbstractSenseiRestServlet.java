package com.sensei.search.client.servlet;

import java.io.OutputStream;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.configuration.DataConfiguration;
import org.apache.commons.configuration.web.ServletRequestConfiguration;

import com.sensei.search.req.SenseiRequest;
import com.sensei.search.req.SenseiResult;

public abstract class AbstractSenseiRestServlet extends AbstractSenseiClientServlet {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	abstract protected SenseiRequest buildSenseiRequest(DataConfiguration params) throws Exception;
	
	@Override
	protected SenseiRequest buildSenseiRequest(HttpServletRequest req)
			throws Exception {
		DataConfiguration params = new DataConfiguration(new ServletRequestConfiguration(req));
		return buildSenseiRequest(params);
	}
	
	abstract protected String buildResultString(SenseiRequest req,SenseiResult res) throws Exception;

	@Override
	protected void convertResult(SenseiRequest req,SenseiResult res, OutputStream ostream)
			throws Exception {
		String outString = buildResultString(req,res);
		ostream.write(outString.getBytes("UTF-8"));
	}
}
