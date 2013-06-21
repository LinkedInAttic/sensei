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
  public static final String SENSEI_CONF_NC_OUTLIER_MULTIPLIER = "sensei.search.cluster.network.outlier.multiplier";
  public static final String SENSEI_CONF_NC_OUTLIER_CONSTANT = "sensei.search.cluster.network.outlier.constant";
  public static final String SENSEI_CONF_NC_CONN_TIMEOUT = "sensei.search.cluster.network.conn.timeout";
	public static final String SENSEI_CONF_NC_WRITE_TIMEOUT = "sensei.search.cluster.network.write.timeout";
	public static final String SENSEI_CONF_NC_MAX_CONN_PER_NODE = "sensei.search.cluster.network.max.conn.per.node";
	public static final String SENSEI_CONF_NC_STALE_TIMEOUT_MINS = "sensei.search.cluster.network.stale.timeout.mins";
	public static final String SENSEI_CONF_NC_STALE_CLEANUP_FREQ_MINS = "sensei.search.cluster.network.stale.cleanup.freq.mins";
	public static final String SENSEI_CONF_VERSION_COMPARATOR = "sensei.search.version.comparator";
	public static final String SENSEI_CONF_ROUTER_FACTORY = "sensei.search.router.factory";
  public static final String SENSEI_CONF_SERIALIZER = "sensei.search.serializer";
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
