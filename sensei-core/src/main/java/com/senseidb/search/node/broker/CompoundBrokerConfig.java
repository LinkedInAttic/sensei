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
package com.senseidb.search.node.broker;

import java.util.Map;

import com.linkedin.norbert.network.Serializer;
import com.senseidb.search.req.SenseiRequest;
import com.senseidb.search.req.SenseiResult;
import org.apache.commons.configuration.Configuration;

import com.linkedin.norbert.javacompat.network.PartitionedLoadBalancerFactory;
import com.senseidb.conf.SenseiConfParams;
import com.senseidb.servlet.SenseiConfigServletContextListener;

public class CompoundBrokerConfig extends BrokerConfig {
  public CompoundBrokerConfig(Configuration senseiConf,
                              PartitionedLoadBalancerFactory<String> loadBalancerFactory,
                              Serializer<SenseiRequest, SenseiResult> serializer,
                              Map<String,String> config, String clusterName) {
    super(senseiConf, loadBalancerFactory, serializer);
    
    this.clusterName = clusterName;
    zkurl = getStrParam(clusterName, config, SenseiConfParams.SENSEI_CLUSTER_URL, zkurl);
    zkTimeout = getIntParam(clusterName, config, SenseiConfParams.SENSEI_CLUSTER_TIMEOUT, zkTimeout);   
    connectTimeoutMillis = getIntParam(clusterName, config, SenseiConfigServletContextListener.SENSEI_CONF_NC_CONN_TIMEOUT, connectTimeoutMillis);   
    writeTimeoutMillis = getIntParam(clusterName, config, SenseiConfigServletContextListener.SENSEI_CONF_NC_WRITE_TIMEOUT, writeTimeoutMillis); 
    maxConnectionsPerNode = getIntParam(clusterName, config, SenseiConfigServletContextListener.SENSEI_CONF_NC_MAX_CONN_PER_NODE, maxConnectionsPerNode); 
    staleRequestTimeoutMins = getIntParam(clusterName, config, SenseiConfigServletContextListener.SENSEI_CONF_NC_STALE_TIMEOUT_MINS, staleRequestTimeoutMins); 
    staleRequestCleanupFrequencyMins = getIntParam(clusterName, config, SenseiConfigServletContextListener.SENSEI_CONF_NC_STALE_CLEANUP_FREQ_MINS, staleRequestCleanupFrequencyMins); 
    
  }
  private String getStrParam(String clusterName, Map<String,String> config, String paramName, String defaultParam) {
    return config.containsKey(clusterName + "." + paramName) ? config.get(clusterName + "." + paramName.substring(paramName.lastIndexOf(".") + 1)) : defaultParam;
  }
  private Integer getIntParam(String clusterName, Map<String,String> config, String paramName, int defaultParam) {
    return config.containsKey(clusterName + "." + paramName) ? Integer.parseInt(config.get(clusterName + "." + paramName.substring(paramName.lastIndexOf(".") + 1))) : defaultParam;
  }
}
