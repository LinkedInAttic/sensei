package com.senseidb.servlet;

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

  public static final String SENSEI_CONF_DIR_PARAM = "config.dir";
	
	public static final String SENSEI_CONF_ZKURL = "sensei.search.cluster.zookeeper.url";
	public static final String SENSEI_CONF_CLUSTER_CLIENT_NAME = "sensei.search.cluster.client-name";
	public static final String SENSEI_CONF_CLUSTER_NAME = "sensei.search.cluster.name";
	public static final String SENSEI_CONF_ZKTIMEOUT = "sensei.search.cluster.zookeeper.conn.timeout";
	public static final String SENSEI_CONF_NC_CONN_TIMEOUT = "sensei.search.cluster.network.conn.timeout";
	public static final String SENSEI_CONF_NC_WRITE_TIMEOUT = "sensei.search.cluster.network.write.timeout";
	public static final String SENSEI_CONF_NC_MAX_CONN_PER_NODE = "sensei.search.cluster.network.max.conn.per.node";
	public static final String SENSEI_CONF_NC_STALE_TIMEOUT_MINS = "sensei.search.cluster.network.stale.timeout.mins";
	public static final String SENSEI_CONF_NC_STALE_CLEANUP_FREQ_MINS = "sensei.search.cluster.network.stale.cleanup.freq.mins";
	public static final String SENSEI_CONF_VERSION_COMPARATOR = "sensei.search.version.comparator";
	public static final String SENSEI_CONF_ROUTER_FACTORY = "sensei.search.router.factory";
	public static final String SENSEI_CONF_OBJ = "sensei.search.configuration";
	public static final String SENSEI_CONF_PLUGIN_REGISTRY = "sensei.search.pluginRegistry";
	@Override
	public void contextDestroyed(ServletContextEvent ctx) {
		
	}

	@Override
	public void contextInitialized(ServletContextEvent ctxEvt) {
		ServletContext ctx = ctxEvt.getServletContext();
		String confFileName = ctx.getInitParameter(SENSEI_CONF_FILE_PARAM);
		

    File confFile = null;
		if (confFileName==null){
		  String confDirName = ctx.getInitParameter(SENSEI_CONF_DIR_PARAM);
		  if (confDirName!=null){
		    confFile = new File(confDirName,"sensei.properties");
		  }
		}
		else{
		  confFile = new File(confFileName);
		}

    if (confFile != null) {
      try {
        PropertiesConfiguration conf = new PropertiesConfiguration();
        conf.setDelimiterParsingDisabled(true);
        conf.load(confFile);
        ctx.setAttribute(SENSEI_CONF_OBJ, conf);
      } 
      catch (ConfigurationException e) {
          logger.error(e.getMessage(),e);
      }
    }
    else {
      logger.warn("configuration is not set.");
    }
	}

}
