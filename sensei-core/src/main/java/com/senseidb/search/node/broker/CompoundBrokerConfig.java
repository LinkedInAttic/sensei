package com.senseidb.search.node.broker;

import java.util.Map;

import org.apache.commons.configuration.Configuration;

import com.linkedin.norbert.javacompat.network.PartitionedLoadBalancerFactory;
import com.senseidb.conf.SenseiConfParams;
import com.senseidb.servlet.SenseiConfigServletContextListener;

public class CompoundBrokerConfig extends BrokerConfig {
  public CompoundBrokerConfig(Configuration senseiConf, PartitionedLoadBalancerFactory<String> loadBalancerFactory, Map<String,String> config, String clusterName) {
    super(senseiConf, loadBalancerFactory);
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
