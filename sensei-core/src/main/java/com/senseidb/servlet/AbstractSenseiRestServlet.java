package com.senseidb.servlet;

import java.io.OutputStream;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.configuration.DataConfiguration;
import org.apache.commons.configuration.web.ServletRequestConfiguration;

import com.senseidb.search.req.SenseiRequest;
import com.senseidb.search.req.SenseiResult;
import com.senseidb.search.req.SenseiSystemInfo;

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
	
	abstract protected String buildResultString(HttpServletRequest httpReq, SenseiRequest req,SenseiResult res) throws Exception;

	abstract protected String buildResultString(HttpServletRequest httpReq, SenseiSystemInfo info) throws Exception;

	@Override
	protected void convertResult(HttpServletRequest httpReq, SenseiSystemInfo info, OutputStream ostream)
			throws Exception {
		String outString = buildResultString(httpReq, info);
		ostream.write(outString.getBytes("UTF-8"));
	}

	@Override
	protected void convertResult(HttpServletRequest httpReq, SenseiRequest req,SenseiResult res, OutputStream ostream)
			throws Exception {
		String outString = buildResultString(httpReq, req,res);
		ostream.write(outString.getBytes("UTF-8"));
	}
}
