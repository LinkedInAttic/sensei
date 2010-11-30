package com.sensei.search.client.servlet;

import java.io.File;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.Logger;

public class SenseiConfigServletContextListener implements
		ServletContextListener {

	private static final Logger logger = Logger.getLogger(SenseiConfigServletContextListener.class);
	
	public static final String SENSEI_CONF_FILE_PARAM = "config.file";
	
	public static final String SENSEI_CONF_ZKURL = "sensei.search.cluster.zookeeper.url";
	public static final String SENSEI_CONF_CLUSTER_NAME = "sensei.search.cluster.name";
	public static final String SENSEI_CONF_ZKTIMEOUT = "sensei.search.cluster.zookeeper.conn.timeout";
	
	@Override
	public void contextDestroyed(ServletContextEvent ctx) {
		
	}

	@Override
	public void contextInitialized(ServletContextEvent ctxEvt) {
		ServletContext ctx = ctxEvt.getServletContext();
		String confFileName = ctx.getInitParameter(SENSEI_CONF_FILE_PARAM);

		File confFile = new File(confFileName);
		try {
			PropertiesConfiguration conf = new PropertiesConfiguration(confFile);
			
			ctx.setAttribute(SENSEI_CONF_ZKURL, conf.getString(SENSEI_CONF_ZKURL));

			ctx.setAttribute(SENSEI_CONF_CLUSTER_NAME, conf.getString(SENSEI_CONF_CLUSTER_NAME));
			ctx.setAttribute(SENSEI_CONF_ZKTIMEOUT, conf.getInt(SENSEI_CONF_ZKTIMEOUT,10000));
		} 
		catch (ConfigurationException e) {
		    logger.error(e.getMessage(),e);
		}
	}

}
