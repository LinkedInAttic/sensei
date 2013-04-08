package com.senseidb.servlet;

import java.util.Comparator;

import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;

import com.linkedin.norbert.javacompat.network.PartitionedLoadBalancer;
import com.linkedin.norbert.javacompat.network.PartitionedLoadBalancerFactory;
import com.linkedin.norbert.network.Serializer;
import com.senseidb.search.req.SenseiRequest;
import com.senseidb.search.req.SenseiResult;
import org.apache.commons.configuration.Configuration;

import com.senseidb.conf.SenseiConfParams;
import com.senseidb.plugin.SenseiPluginRegistry;

public class ZookeeperConfigurableServlet extends HttpServlet {
  
  /**
   * 
   */
  private static final long serialVersionUID = 1L;

  protected PartitionedLoadBalancerFactory<String> loadBalancerFactory;
  protected Serializer<SenseiRequest, SenseiResult> serializer;
  protected Comparator<String> versionComparator;
  protected boolean allowPartialMerge;

  protected Configuration senseiConf;

  protected SenseiPluginRegistry pluginRegistry;

  @Override
  public void init(ServletConfig config) throws ServletException {
    super.init(config);
    
    ServletContext ctx = config.getServletContext();
     senseiConf = (Configuration)ctx.getAttribute(SenseiConfigServletContextListener.SENSEI_CONF_OBJ);
    
    versionComparator = (Comparator<String>)ctx.getAttribute(SenseiConfigServletContextListener.SENSEI_CONF_VERSION_COMPARATOR);
    loadBalancerFactory = (PartitionedLoadBalancerFactory<String>) ctx.getAttribute(SenseiConfigServletContextListener.SENSEI_CONF_ROUTER_FACTORY);
    serializer = (Serializer<SenseiRequest, SenseiResult>) ctx.getAttribute(SenseiConfigServletContextListener.SENSEI_CONF_SERIALIZER);
    pluginRegistry = (SenseiPluginRegistry) ctx.getAttribute(SenseiConfigServletContextListener.SENSEI_CONF_PLUGIN_REGISTRY);
    allowPartialMerge = senseiConf.getBoolean(SenseiConfParams.ALLOW_PARTIAL_MERGE, true);
}
}
