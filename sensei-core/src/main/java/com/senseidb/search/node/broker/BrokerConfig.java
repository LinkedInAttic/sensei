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


import com.linkedin.norbert.javacompat.cluster.ZooKeeperClusterClient;
import com.linkedin.norbert.javacompat.network.NetworkClientConfig;
import com.linkedin.norbert.javacompat.network.PartitionedLoadBalancerFactory;
import com.senseidb.cluster.client.SenseiNetworkClient;
import com.senseidb.conf.SenseiConfParams;
import com.senseidb.plugin.SenseiPluginRegistry;
import com.senseidb.search.node.SenseiBroker;
import com.senseidb.search.node.SenseiSysBroker;
import com.senseidb.search.req.SenseiRequestCustomizerFactory;
import com.senseidb.servlet.SenseiConfigServletContextListener;
import java.util.Comparator;
import org.apache.commons.configuration.Configuration;

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
  private long brokerTimeout;
  private SenseiRequestCustomizerFactory requestCustomizerFactory;


  public BrokerConfig(Configuration senseiConf, PartitionedLoadBalancerFactory<String> loadBalancerFactory, SenseiPluginRegistry pluginRegistry) {
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
    brokerTimeout = senseiConf.getLong(SenseiConfParams.SERVER_BROKER_TIMEOUT, 8000);
    requestCustomizerFactory = pluginRegistry.getBeanByFullPrefix(SenseiConfParams.SERVER_BROKER_REQUEST_CUSTOMIZER_FACTORY, SenseiRequestCustomizerFactory.class);
  }
  
  public BrokerConfig(Configuration senseiConf, PartitionedLoadBalancerFactory<String> loadBalancerFactory) {
    this(senseiConf, loadBalancerFactory, null);
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
    senseiBroker = new SenseiBroker(networkClient, clusterClient, allowPartialMerge, requestCustomizerFactory);
    senseiBroker.setTimeout(brokerTimeout);
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

  public void setClusterName(String clusterName) {
    this.clusterName = clusterName;
  }

  public void setZkurl(String zkurl) {
    this.zkurl = zkurl;
  }

  public void setZkTimeout(int zkTimeout) {
    this.zkTimeout = zkTimeout;
  }

  public void setWriteTimeoutMillis(int writeTimeoutMillis) {
    this.writeTimeoutMillis = writeTimeoutMillis;
  }

  public void setConnectTimeoutMillis(int connectTimeoutMillis) {
    this.connectTimeoutMillis = connectTimeoutMillis;
  }

  public void setMaxConnectionsPerNode(int maxConnectionsPerNode) {
    this.maxConnectionsPerNode = maxConnectionsPerNode;
  }

  public void setStaleRequestTimeoutMins(int staleRequestTimeoutMins) {
    this.staleRequestTimeoutMins = staleRequestTimeoutMins;
  }

  public void setStaleRequestCleanupFrequencyMins(int staleRequestCleanupFrequencyMins) {
    this.staleRequestCleanupFrequencyMins = staleRequestCleanupFrequencyMins;
  }

  public void setLoadBalancerFactory(PartitionedLoadBalancerFactory<String> loadBalancerFactory) {
    this.loadBalancerFactory = loadBalancerFactory;
  }

  public void setAllowPartialMerge(boolean allowPartialMerge) {
    this.allowPartialMerge = allowPartialMerge;
  }


  
}
