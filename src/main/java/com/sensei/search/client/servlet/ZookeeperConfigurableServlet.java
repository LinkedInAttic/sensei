package com.sensei.search.client.servlet;

import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;

import org.apache.commons.configuration.Configuration;

public class ZookeeperConfigurableServlet extends HttpServlet {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	protected String zkurl;
	protected String clusterName;
	protected int zkTimeout;

	@Override
	public void init(ServletConfig config) throws ServletException {
		super.init(config);
		
		ServletContext ctx = config.getServletContext();
		
		Configuration senseConf = (Configuration)ctx.getAttribute(SenseiConfigServletContextListener.SENSEI_CONF_OBJ);
			
		zkurl = senseConf.getString(SenseiConfigServletContextListener.SENSEI_CONF_ZKURL);
		clusterName = senseConf.getString(SenseiConfigServletContextListener.SENSEI_CONF_CLUSTER_NAME);
		zkTimeout = senseConf.getInt(SenseiConfigServletContextListener.SENSEI_CONF_ZKTIMEOUT,10000);
	}
}
