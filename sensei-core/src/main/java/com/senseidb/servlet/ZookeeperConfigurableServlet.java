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
