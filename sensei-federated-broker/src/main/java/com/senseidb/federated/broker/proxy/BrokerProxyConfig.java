package com.senseidb.federated.broker.proxy;

import java.util.Map;

import org.apache.commons.configuration.Configuration;

import com.linkedin.norbert.javacompat.network.PartitionedLoadBalancerFactory;
import com.senseidb.conf.SenseiConfParams;
import com.senseidb.search.node.broker.BrokerConfig;
import com.senseidb.servlet.SenseiConfigServletContextListener;

public class BrokerProxyConfig extends BrokerConfig {
  public BrokerProxyConfig(Configuration senseiConf, PartitionedLoadBalancerFactory<String> loadBalancerFactory, Map<String,String> config) {
    super(senseiConf, loadBalancerFactory);
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
