package com.sensei.search.client.servlet;

import java.util.Comparator;

import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;

import com.sensei.search.cluster.routing.SenseiLoadBalancerFactory;

import org.apache.commons.configuration.Configuration;

public class ZookeeperConfigurableServlet extends HttpServlet {
  
  /**
   * 
   */
  private static final long serialVersionUID = 1L;
  protected String zkurl;
	protected String clusterClientName;
  protected String clusterName;
  protected int zkTimeout;
  protected int connectTimeoutMillis;
  protected int writeTimeoutMillis;
  protected int maxConnectionsPerNode;
  protected int staleRequestTimeoutMins;
  protected int staleRequestCleanupFrequencyMins;
  protected SenseiLoadBalancerFactory loadBalancerFactory;
  protected Comparator<String> versionComparator;

  @Override
  public void init(ServletConfig config) throws ServletException {
    super.init(config);
    
    ServletContext ctx = config.getServletContext();
    
    Configuration senseiConf = (Configuration)ctx.getAttribute(SenseiConfigServletContextListener.SENSEI_CONF_OBJ);
    zkurl = senseiConf.getString(SenseiConfigServletContextListener.SENSEI_CONF_ZKURL);
	  clusterClientName = senseiConf.getString(SenseiConfigServletContextListener.SENSEI_CONF_CLUSTER_CLIENT_NAME);
    clusterName = senseiConf.getString(SenseiConfigServletContextListener.SENSEI_CONF_CLUSTER_NAME);
    zkTimeout = senseiConf.getInt(SenseiConfigServletContextListener.SENSEI_CONF_ZKTIMEOUT,10000);
    connectTimeoutMillis = senseiConf.getInt(SenseiConfigServletContextListener.SENSEI_CONF_NC_CONN_TIMEOUT, 1000);
    writeTimeoutMillis = senseiConf.getInt(SenseiConfigServletContextListener.SENSEI_CONF_NC_WRITE_TIMEOUT, 150);
    maxConnectionsPerNode = senseiConf.getInt(SenseiConfigServletContextListener.SENSEI_CONF_NC_MAX_CONN_PER_NODE, 5);
    staleRequestTimeoutMins = senseiConf.getInt(SenseiConfigServletContextListener.SENSEI_CONF_NC_STALE_TIMEOUT_MINS, 10);
    staleRequestCleanupFrequencyMins = senseiConf.getInt(SenseiConfigServletContextListener.SENSEI_CONF_NC_STALE_CLEANUP_FREQ_MINS, 10);

    versionComparator = (Comparator<String>)ctx.getAttribute(SenseiConfigServletContextListener.SENSEI_CONF_VERSION_COMPARATOR);
    loadBalancerFactory = (SenseiLoadBalancerFactory)ctx.getAttribute(
        SenseiConfigServletContextListener.SENSEI_CONF_ROUTER_FACTORY);
  }
}
