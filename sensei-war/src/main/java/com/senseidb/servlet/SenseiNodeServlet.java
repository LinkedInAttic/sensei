package com.senseidb.servlet;

import java.io.File;

import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;

import com.linkedin.norbert.javacompat.network.IntegerConsistentHashPartitionedLoadBalancerFactory;
import com.linkedin.norbert.javacompat.network.PartitionedLoadBalancerFactory;
import com.senseidb.cluster.routing.SenseiPartitionedLoadBalancerFactory;
import com.senseidb.conf.SenseiConfParams;
import com.senseidb.conf.SenseiServerBuilder;
import com.senseidb.plugin.SenseiPluginRegistry;
import com.senseidb.search.node.SenseiServer;
import com.senseidb.servlet.DefaultSenseiJSONServlet;
import com.senseidb.servlet.SenseiConfigServletContextListener;

public class SenseiNodeServlet extends DefaultSenseiJSONServlet {

  /**
   * 
   */
  private static final long serialVersionUID = 1L;
  private SenseiServer _senseiServer = null;
  @Override
  public void init(ServletConfig config) throws ServletException {
    
    ServletContext ctx = config.getServletContext();
    
    String confDirName = ctx.getInitParameter(SenseiConfigServletContextListener.SENSEI_CONF_DIR_PARAM);
    if (confDirName==null){
      throw new ServletException("configuration not specified, "+SenseiConfigServletContextListener.SENSEI_CONF_DIR_PARAM+" not set");
    }
    
    SenseiServerBuilder builder;
    try {
      builder = new SenseiServerBuilder(new File(confDirName), null);
      ctx.setAttribute("sensei.search.configuration", builder.getConfiguration());
      ctx.setAttribute("sensei.search.version.comparator",builder.getVersionComparator());
      SenseiPluginRegistry pluginRegistry = builder.getPluginRegistry();
      PartitionedLoadBalancerFactory<String> routerFactory = pluginRegistry.getBeanByFullPrefix(SenseiConfParams.SERVER_SEARCH_ROUTER_FACTORY, PartitionedLoadBalancerFactory.class);
      if (routerFactory == null) {
        routerFactory = new SenseiPartitionedLoadBalancerFactory(50);
      }
      ctx.setAttribute("sensei.search.router.factory", routerFactory);

      _senseiServer = builder.buildServer();
      _senseiServer.start(true);
      super.init(config);
    } catch (Exception e) {
      throw new ServletException(e.getMessage(),e);
    }    
  }

  @Override
  public void destroy() {
    if (_senseiServer!=null){
      _senseiServer.shutdown();
    }
    super.destroy();
  }

}
