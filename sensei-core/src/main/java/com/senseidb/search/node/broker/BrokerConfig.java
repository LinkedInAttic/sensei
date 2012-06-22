package com.senseidb.search.node.broker;

import java.util.Comparator;

import org.apache.commons.configuration.Configuration;

import com.linkedin.norbert.javacompat.cluster.ZooKeeperClusterClient;
import com.linkedin.norbert.javacompat.network.NetworkClientConfig;
import com.linkedin.norbert.javacompat.network.PartitionedLoadBalancerFactory;
import com.senseidb.cluster.client.SenseiNetworkClient;
import com.senseidb.conf.SenseiConfParams;
import com.senseidb.search.node.SenseiBroker;
import com.senseidb.search.node.SenseiSysBroker;
import com.senseidb.search.req.SenseiRequest;
import com.senseidb.servlet.SenseiConfigServletContextListener;
import com.senseidb.svc.api.SenseiException;

public class BrokerConfig {
  protected String clusterName;
  protected String zkurl;
  protected int zkTimeout;
  protected int writeTimeoutMillis;
  protected int connectTimeoutMillis;
  protected int maxConnectionsPerNode;
  protected int staleRequestTimeoutMins;
  protected int staleRequestCleanupFrequencyMins;

  protected PartitionedLoadBalancerFactory<String> loadBalancerFactory;
  private final NetworkClientConfig networkClientConfig = new NetworkClientConfig();
  protected boolean allowPartialMerge;
  private ZooKeeperClusterClient clusterClient;
  private SenseiNetworkClient networkClient;
  private SenseiBroker senseiBroker;
  private SenseiSysBroker senseiSysBroker;

  public BrokerConfig(Configuration senseiConf, PartitionedLoadBalancerFactory<String> loadBalancerFactory) {
    this.loadBalancerFactory = loadBalancerFactory;
    clusterName = senseiConf.getString(SenseiConfParams.SENSEI_CLUSTER_NAME);
    zkurl = senseiConf.getString(SenseiConfParams.SENSEI_CLUSTER_URL);
    zkTimeout = senseiConf.getInt(SenseiConfParams.SENSEI_CLUSTER_TIMEOUT, 300000);
    zkurl = senseiConf.getString(SenseiConfigServletContextListener.SENSEI_CONF_ZKURL, zkurl);
    clusterName = senseiConf.getString(SenseiConfigServletContextListener.SENSEI_CONF_CLUSTER_NAME, clusterName);
    zkTimeout = senseiConf.getInt(SenseiConfigServletContextListener.SENSEI_CONF_ZKTIMEOUT, zkTimeout);
    connectTimeoutMillis = senseiConf.getInt(SenseiConfigServletContextListener.SENSEI_CONF_NC_CONN_TIMEOUT, 1000);
    writeTimeoutMillis = senseiConf.getInt(SenseiConfigServletContextListener.SENSEI_CONF_NC_WRITE_TIMEOUT, 150);
    maxConnectionsPerNode = senseiConf.getInt(SenseiConfigServletContextListener.SENSEI_CONF_NC_MAX_CONN_PER_NODE, 5);
    staleRequestTimeoutMins = senseiConf.getInt(SenseiConfigServletContextListener.SENSEI_CONF_NC_STALE_TIMEOUT_MINS, 10);
    staleRequestCleanupFrequencyMins = senseiConf.getInt(SenseiConfigServletContextListener.SENSEI_CONF_NC_STALE_CLEANUP_FREQ_MINS, 10);
    allowPartialMerge = senseiConf.getBoolean(SenseiConfParams.ALLOW_PARTIAL_MERGE, true); 
  }

  public void init() {
    networkClientConfig.setServiceName(clusterName);
    networkClientConfig.setZooKeeperConnectString(zkurl);
    networkClientConfig.setZooKeeperSessionTimeoutMillis(zkTimeout);
    networkClientConfig.setConnectTimeoutMillis(connectTimeoutMillis);
    networkClientConfig.setWriteTimeoutMillis(writeTimeoutMillis);
    networkClientConfig.setMaxConnectionsPerNode(maxConnectionsPerNode);
    networkClientConfig.setStaleRequestTimeoutMins(staleRequestTimeoutMins);
    networkClientConfig.setStaleRequestCleanupFrequencyMins(staleRequestCleanupFrequencyMins);
    clusterClient = new ZooKeeperClusterClient(clusterName, zkurl, zkTimeout);
    networkClientConfig.setClusterClient(clusterClient);
    networkClient = new SenseiNetworkClient(networkClientConfig, this.loadBalancerFactory);
    clusterClient.awaitConnectionUninterruptibly();
  }

  public SenseiBroker buildSenseiBroker() {   
    senseiBroker = new SenseiBroker(networkClient, clusterClient, allowPartialMerge);
    
    return senseiBroker;
  }
  public SenseiSysBroker buildSysSenseiBroker(Comparator<String> versionComparator) {   
     senseiSysBroker = new SenseiSysBroker(networkClient, clusterClient, versionComparator, allowPartialMerge);
    return senseiSysBroker;
  }

  public ZooKeeperClusterClient getClusterClient() {
    return clusterClient;
  }

  public SenseiNetworkClient getNetworkClient() {
    return networkClient;
  }

  public SenseiBroker getSenseiBroker() {
    return senseiBroker;
  }

  public SenseiSysBroker getSenseiSysBroker() {
    return senseiSysBroker;
  }

  public String getClusterName() {
    return clusterName;
  }
  
}
