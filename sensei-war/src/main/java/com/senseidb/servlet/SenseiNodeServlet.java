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
