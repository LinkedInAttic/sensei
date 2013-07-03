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

package com.senseidb.federated.broker.proxy;

import java.util.Map;

import com.linkedin.norbert.network.Serializer;
import com.senseidb.search.req.SenseiRequest;
import com.senseidb.search.req.SenseiResult;
import org.apache.commons.configuration.Configuration;

import com.linkedin.norbert.javacompat.network.PartitionedLoadBalancerFactory;
import com.senseidb.conf.SenseiConfParams;
import com.senseidb.search.node.broker.BrokerConfig;
import com.senseidb.servlet.SenseiConfigServletContextListener;

public class BrokerProxyConfig extends BrokerConfig {
  public BrokerProxyConfig(Configuration senseiConf,
                           PartitionedLoadBalancerFactory<String> loadBalancerFactory,
                           Serializer<SenseiRequest, SenseiResult> serializer,
                           Map<String,String> config) {
    super(senseiConf, loadBalancerFactory, serializer);
    clusterName = getStrParam(config, SenseiConfParams.SENSEI_CLUSTER_NAME, clusterName);    
    zkurl = getStrParam(config, SenseiConfParams.SENSEI_CLUSTER_URL, zkurl);
    zkTimeout = getIntParam(config, SenseiConfParams.SENSEI_CLUSTER_TIMEOUT, zkTimeout);   
    connectTimeoutMillis = getIntParam(config, SenseiConfigServletContextListener.SENSEI_CONF_NC_CONN_TIMEOUT, connectTimeoutMillis);   
    writeTimeoutMillis = getIntParam(config, SenseiConfigServletContextListener.SENSEI_CONF_NC_WRITE_TIMEOUT, writeTimeoutMillis); 
    maxConnectionsPerNode = getIntParam(config, SenseiConfigServletContextListener.SENSEI_CONF_NC_MAX_CONN_PER_NODE, maxConnectionsPerNode); 
    staleRequestTimeoutMins = getIntParam(config, SenseiConfigServletContextListener.SENSEI_CONF_NC_STALE_TIMEOUT_MINS, staleRequestTimeoutMins); 
    staleRequestCleanupFrequencyMins = getIntParam(config, SenseiConfigServletContextListener.SENSEI_CONF_NC_STALE_CLEANUP_FREQ_MINS, staleRequestCleanupFrequencyMins); 
  }
  private String getStrParam(Map<String,String> config, String paramName, String defaultParam) {
    return config.containsKey(paramName) ? config.get(paramName) : defaultParam;
  }
  private Integer getIntParam(Map<String,String> config, String paramName, int defaultParam) {
    return config.containsKey(paramName) ? Integer.parseInt(config.get(paramName)) : defaultParam;
  }

}
