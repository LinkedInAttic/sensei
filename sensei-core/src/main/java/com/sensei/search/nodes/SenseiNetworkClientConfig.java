/**
 * 
 */
package com.sensei.search.nodes;

import com.linkedin.norbert.javacompat.network.NetworkClientConfig;

/**
 * @author nnarkhed
 *
 */
public class SenseiNetworkClientConfig
{
  private String _serviceName;
  private String _zooKeeperURL;
  private int _zooKeeperSessionTimeoutMillis;
  private int _connectTimeoutMillis;
  private int _writeTimeoutMillis;
  private int _maxConnectionsPerNode;
  private int _staleRequestTimeoutMins;
  private int _staleRequestCleanupFrequencyMins;
  
  /**
   * @return the serviceName
   */
  public String getserviceName()
  {
    return _serviceName;
  }
  /**
   * @param serviceName the serviceName to set
   */
  public void setserviceName(String serviceName)
  {
    _serviceName = serviceName;
  }
  /**
   * @return the zookeeperURL
   */
  public String getZooKeeperURL()
  {
    return _zooKeeperURL;
  }
  /**
   * @param zookeeperURL the zookeeperURL to set
   */
  public void setZooKeeperURL(String zookeeperURL)
  {
    _zooKeeperURL = zookeeperURL;
  }
  /**
   * @return the zooKeeperSessionTimeoutMillis
   */
  public int getZooKeeperSessionTimeoutMillis()
  {
    return _zooKeeperSessionTimeoutMillis;
  }
  /**
   * @param zooKeeperSessionTimeoutMillis the zooKeeperSessionTimeoutMillis to set
   */
  public void setZooKeeperSessionTimeoutMillis(int zooKeeperSessionTimeoutMillis)
  {
    _zooKeeperSessionTimeoutMillis = zooKeeperSessionTimeoutMillis;
  }
  /**
   * @return the _connectTimeoutMillis
   */
  public int getConnectTimeoutMillis()
  {
    return _connectTimeoutMillis;
  }
  /**
   * @param connectTimeoutMillis the connectTimeoutMillis to set
   */
  public void setConnectTimeoutMillis(int connectTimeoutMillis)
  {
    this._connectTimeoutMillis = connectTimeoutMillis;
  }
  /**
   * @return the writeTimeoutMillis
   */
  public int getWriteTimeoutMillis()
  {
    return _writeTimeoutMillis;
  }
  /**
   * @param writeTimeoutMillis the writeTimeoutMillis to set
   */
  public void setWriteTimeoutMillis(int writeTimeoutMillis)
  {
    this._writeTimeoutMillis = writeTimeoutMillis;
  }
  /**
   * @return the maxConnectionsPerNode
   */
  public int getMaxConnectionsPerNode()
  {
    return _maxConnectionsPerNode;
  }
  /**
   * @param maxConnectionsPerNode the maxConnectionsPerNode to set
   */
  public void setMaxConnectionsPerNode(int maxConnectionsPerNode)
  {
    this._maxConnectionsPerNode = maxConnectionsPerNode;
  }
  /**
   * @return the staleRequestTimeoutMins
   */
  public int getStaleRequestTimeoutMins()
  {
    return _staleRequestTimeoutMins;
  }
  /**
   * @param staleRequestTimeoutMins the staleRequestTimeoutMins to set
   */
  public void setStaleRequestTimeoutMins(int staleRequestTimeoutMins)
  {
    this._staleRequestTimeoutMins = staleRequestTimeoutMins;
  }
  /**
   * @return the staleRequestCleanupFrequencyMins
   */
  public int getStaleRequestCleanupFrequencyMins()
  {
    return _staleRequestCleanupFrequencyMins;
  }
  /**
   * @param staleRequestCleanupFrequencyMins the staleRequestCleanupFrequencyMins to set
   */
  public void setStaleRequestCleanupFrequencyMins(int staleRequestCleanupFrequencyMins)
  {
    this._staleRequestCleanupFrequencyMins = staleRequestCleanupFrequencyMins;
  }
  
  public NetworkClientConfig getNetworkConfigObject()
  {
    NetworkClientConfig netConfig = new NetworkClientConfig();
    
    netConfig.setServiceName(_serviceName);
    netConfig.setZooKeeperSessionTimeoutMillis(_zooKeeperSessionTimeoutMillis);
    netConfig.setZooKeeperConnectString(_zooKeeperURL);
    netConfig.setConnectTimeoutMillis(_connectTimeoutMillis);
    netConfig.setMaxConnectionsPerNode(_maxConnectionsPerNode);
    netConfig.setStaleRequestCleanupFrequencyMins(_staleRequestCleanupFrequencyMins);
    netConfig.setStaleRequestTimeoutMins(_staleRequestTimeoutMins);
    netConfig.setWriteTimeoutMillis(_writeTimeoutMillis);
    
    return netConfig;
  }
}
