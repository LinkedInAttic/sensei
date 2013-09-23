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
import com.linkedin.norbert.network.Serializer;
import com.senseidb.cluster.client.SenseiNetworkClient;
import com.senseidb.conf.SenseiConfParams;
import com.senseidb.search.node.SenseiBroker;
import com.senseidb.search.node.SenseiSysBroker;
import com.senseidb.search.req.SenseiRequestCustomizerFactory;
import com.senseidb.search.req.SenseiResult;
import com.senseidb.servlet.SenseiConfigServletContextListener;
import org.apache.commons.configuration.Configuration;

import java.util.Comparator;

public class BrokerConfig {
  protected String clusterName;
  protected String zkurl;
  protected int zkTimeout;
  protected int writeTimeoutMillis;
  protected int connectTimeoutMillis;
  protected int maxConnectionsPerNode;
  protected int staleRequestTimeoutMins;
  protected int staleRequestCleanupFrequencyMins;
  private double outlierMultiplier;
  private double outlierConstant;

  protected PartitionedLoadBalancerFactory<String> loadBalancerFactory;
  protected Serializer<SenseiRequest, SenseiResult> serializer;
  private final NetworkClientConfig networkClientConfig = new NetworkClientConfig();
  protected boolean allowPartialMerge;
  private ZooKeeperClusterClient clusterClient;
  private SenseiNetworkClient networkClient;
  private SenseiBroker senseiBroker;
  private SenseiSysBroker senseiSysBroker;
  private long brokerTimeout;
  private long brokerTimeout;
  private SenseiRequestCustomizerFactory requestCustomizerFactory;
  protected long brokerTimeout;

  
  public BrokerConfig(Configuration senseiConf,
                      PartitionedLoadBalancerFactory<String> loadBalancerFactory,
                      Serializer<SenseiRequest, SenseiResult> serializer) {
    this.loadBalancerFactory = loadBalancerFactory;
    this.serializer = serializer;
    clusterName = senseiConf.getString(SenseiConfParams.SENSEI_CLUSTER_NAME);
    zkurl = senseiConf.getString(SenseiConfParams.SENSEI_CLUSTER_URL);
    zkTimeout = senseiConf.getInt(SenseiConfParams.SENSEI_CLUSTER_TIMEOUT, 300000);
    zkurl = senseiConf.getString(SenseiConfigServletContextListener.SENSEI_CONF_ZKURL, zkurl);
    clusterName = senseiConf.getString(SenseiConfigServletContextListener.SENSEI_CONF_CLUSTER_NAME, clusterName);
    zkTimeout = senseiConf.getInt(SenseiConfigServletContextListener.SENSEI_CONF_ZKTIMEOUT, zkTimeout);
    outlierMultiplier = senseiConf.getDouble(SenseiConfigServletContextListener.SENSEI_CONF_NC_OUTLIER_MULTIPLIER, 3.0);
    outlierConstant = senseiConf.getDouble(SenseiConfigServletContextListener.SENSEI_CONF_NC_OUTLIER_CONSTANT, 150);

    connectTimeoutMillis = senseiConf.getInt(SenseiConfigServletContextListener.SENSEI_CONF_NC_CONN_TIMEOUT, 1000);
    writeTimeoutMillis = senseiConf.getInt(SenseiConfigServletContextListener.SENSEI_CONF_NC_WRITE_TIMEOUT, 150);
    maxConnectionsPerNode = senseiConf.getInt(SenseiConfigServletContextListener.SENSEI_CONF_NC_MAX_CONN_PER_NODE, 5);
    staleRequestTimeoutMins = senseiConf.getInt(SenseiConfigServletContextListener.SENSEI_CONF_NC_STALE_TIMEOUT_MINS, 10);
    staleRequestCleanupFrequencyMins = senseiConf.getInt(SenseiConfigServletContextListener.SENSEI_CONF_NC_STALE_CLEANUP_FREQ_MINS, 10);
    allowPartialMerge = senseiConf.getBoolean(SenseiConfParams.ALLOW_PARTIAL_MERGE, true); 
    brokerTimeout = senseiConf.getLong(SenseiConfParams.SERVER_BROKER_TIMEOUT, 8000);

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
    networkClientConfig.setOutlierMuliplier(outlierMultiplier);
    networkClientConfig.setOutlierConstant(outlierConstant);
    clusterClient = new ZooKeeperClusterClient(clusterName, zkurl, zkTimeout);
    networkClientConfig.setClusterClient(clusterClient);
    networkClient = new SenseiNetworkClient(networkClientConfig, this.loadBalancerFactory);
    clusterClient.awaitConnectionUninterruptibly();
  }

  public long getBrokerTimeout() {
    return brokerTimeout;
  }

  public boolean isAllowPartialMerge() {
    return allowPartialMerge;
  }

  public SenseiBroker buildSenseiBroker() {
    senseiBroker = new SenseiBroker(networkClient, clusterClient, serializer, brokerTimeout, allowPartialMerge);
    return senseiBroker;
  }
  public SenseiSysBroker buildSysSenseiBroker(Comparator<String> versionComparator) {   
     senseiSysBroker = new SenseiSysBroker(networkClient, clusterClient, versionComparator, brokerTimeout, allowPartialMerge);
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

  public Serializer<SenseiRequest, SenseiResult> getSerializer() {
    return serializer;
  }
  
}
