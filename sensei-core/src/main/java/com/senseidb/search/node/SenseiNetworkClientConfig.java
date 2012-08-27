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
package com.senseidb.search.node;

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
