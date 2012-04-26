package com.senseidb.servlet;

import java.util.Comparator;

import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;

import com.linkedin.norbert.javacompat.network.PartitionedLoadBalancer;
import com.linkedin.norbert.javacompat.network.PartitionedLoadBalancerFactory;
import org.apache.commons.configuration.Configuration;

import com.senseidb.conf.SenseiConfParams;

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
  protected PartitionedLoadBalancerFactory<String> loadBalancerFactory;
  protected Comparator<String> versionComparator;
  protected boolean allowPartialMerge;

  @Override
  public void init(ServletConfig config) throws ServletException {
    super.init(config);
    
    ServletContext ctx = config.getServletContext();
    Configuration senseiConf = (Configuration)ctx.getAttribute(SenseiConfigServletContextListener.SENSEI_CONF_OBJ);
    
    clusterName = senseiConf.getString(SenseiConfParams.SENSEI_CLUSTER_NAME);
    clusterClientName = senseiConf.getString(SenseiConfParams.SENSEI_CLUSTER_CLIENT_NAME,clusterName);
    zkurl = senseiConf.getString(SenseiConfParams.SENSEI_CLUSTER_URL);
    zkTimeout = senseiConf.getInt(SenseiConfParams.SENSEI_CLUSTER_TIMEOUT, 300000);
    
    zkurl = senseiConf.getString(SenseiConfigServletContextListener.SENSEI_CONF_ZKURL,zkurl);
	  clusterClientName = senseiConf.getString(SenseiConfigServletContextListener.SENSEI_CONF_CLUSTER_CLIENT_NAME,clusterClientName);
    clusterName = senseiConf.getString(SenseiConfigServletContextListener.SENSEI_CONF_CLUSTER_NAME,clusterName);
    zkTimeout = senseiConf.getInt(SenseiConfigServletContextListener.SENSEI_CONF_ZKTIMEOUT,zkTimeout);
    
    connectTimeoutMillis = senseiConf.getInt(SenseiConfigServletContextListener.SENSEI_CONF_NC_CONN_TIMEOUT, 1000);
    writeTimeoutMillis = senseiConf.getInt(SenseiConfigServletContextListener.SENSEI_CONF_NC_WRITE_TIMEOUT, 150);
    maxConnectionsPerNode = senseiConf.getInt(SenseiConfigServletContextListener.SENSEI_CONF_NC_MAX_CONN_PER_NODE, 5);
    staleRequestTimeoutMins = senseiConf.getInt(SenseiConfigServletContextListener.SENSEI_CONF_NC_STALE_TIMEOUT_MINS, 10);
    staleRequestCleanupFrequencyMins = senseiConf.getInt(SenseiConfigServletContextListener.SENSEI_CONF_NC_STALE_CLEANUP_FREQ_MINS, 10);

    versionComparator = (Comparator<String>)ctx.getAttribute(SenseiConfigServletContextListener.SENSEI_CONF_VERSION_COMPARATOR);
    loadBalancerFactory = (PartitionedLoadBalancerFactory<String>) ctx.getAttribute(
        SenseiConfigServletContextListener.SENSEI_CONF_ROUTER_FACTORY);

    allowPartialMerge = senseiConf.getBoolean(SenseiConfParams.ALLOW_PARTIAL_MERGE, true);
}
}
