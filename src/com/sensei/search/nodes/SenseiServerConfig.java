/**
 * 
 */
package com.sensei.search.nodes;

import com.linkedin.norbert.cluster.javaapi.ClusterClient;
import com.linkedin.norbert.network.javaapi.NetworkServerConfig;

/**
 * @author nnarkhed
 *
 */
public class SenseiServerConfig
{
  private String _serviceName;
  private String _zooKeeperURL;
  private int _zooKeeperSessionTimeout;
  private int _requestThreadCorePoolSize;
  private int _requestThreadMaxPoolSize;
  private int _requestThreadKeepAliveTimeSecs;
  private ClusterClient _clusterClient;
  
  public SenseiServerConfig()
  {
    _serviceName = null;
    _zooKeeperURL = null;
    _zooKeeperSessionTimeout = 0;
    _requestThreadCorePoolSize = 0;
    _requestThreadMaxPoolSize = 0;
    _requestThreadKeepAliveTimeSecs = 0;
    _clusterClient = null;
  }
  
  public SenseiServerConfig(String serviceName, String zooKeeperURL, int zooKeeperSessionTimeout, int requestThreadCorePoolSize, 
                            int requestThreadMaxPoolSize, int requestThreadKeepAliveTimeSecs)
  {
    _serviceName = serviceName;
    _zooKeeperURL = zooKeeperURL;
    _zooKeeperSessionTimeout = zooKeeperSessionTimeout;
    _requestThreadCorePoolSize = requestThreadCorePoolSize;
    _requestThreadMaxPoolSize = requestThreadMaxPoolSize;
    _requestThreadKeepAliveTimeSecs = requestThreadKeepAliveTimeSecs;
    _clusterClient = null;
  }

  /**
   * @return the serviceName
   */
  public String getServiceName()
  {
    return _serviceName;
  }

  /**
   * @param serviceName the serviceName to set
   */
  public void setServiceName(String serviceName)
  {
    _serviceName = serviceName;
  }

  /**
   * @return the zooKeeperURL
   */
  public String getZooKeeperURL()
  {
    return _zooKeeperURL;
  }

  /**
   * @param zooKeeperURL the zooKeeperURL to set
   */
  public void setZooKeeperURL(String zooKeeperURL)
  {
    _zooKeeperURL = zooKeeperURL;
  }

  /**
   * @return the zooKeeperSessionTimeout
   */
  public int getZooKeeperSessionTimeout()
  {
    return _zooKeeperSessionTimeout;
  }

  /**
   * @param zooKeeperSessionTimeout the zooKeeperSessionTimeout to set
   */
  public void setZooKeeperSessionTimeout(int zooKeeperSessionTimeout)
  {
    _zooKeeperSessionTimeout = zooKeeperSessionTimeout;
  }

  /**
   * @return the requestThreadCorePoolSize
   */
  public int getRequestThreadCorePoolSize()
  {
    return _requestThreadCorePoolSize;
  }

  /**
   * @param requestThreadCorePoolSize the requestThreadCorePoolSize to set
   */
  public void setRequestThreadCorePoolSize(int requestThreadCorePoolSize)
  {
    _requestThreadCorePoolSize = requestThreadCorePoolSize;
  }

  /**
   * @return the requestThreadMaxPoolSize
   */
  public int getRequestThreadMaxPoolSize()
  {
    return _requestThreadMaxPoolSize;
  }

  /**
   * @param requestThreadMaxPoolSize the requestThreadMaxPoolSize to set
   */
  public void setRequestThreadMaxPoolSize(int requestThreadMaxPoolSize)
  {
    _requestThreadMaxPoolSize = requestThreadMaxPoolSize;
  }

  /**
   * @return the requestThreadKeepAliveTimeSecs
   */
  public int getRequestThreadKeepAliveTimeSecs()
  {
    return _requestThreadKeepAliveTimeSecs;
  }

  /**
   * @param requestThreadKeepAliveTimeSecs the requestThreadKeepAliveTimeSecs to set
   */
  public void setRequestThreadKeepAliveTimeSecs(int requestThreadKeepAliveTimeSecs)
  {
    _requestThreadKeepAliveTimeSecs = requestThreadKeepAliveTimeSecs;
  }  
  
  /**
   * @return the clusterClient
   */
  public ClusterClient getClusterClient()
  {
    return _clusterClient;
  }

  /**
   * @param clusterClient the clusterClient to set
   */
  public void setClusterClient(ClusterClient clusterClient)
  {
    _clusterClient = clusterClient;
  }

  public NetworkServerConfig getNetworkServerConfig()
  {
    NetworkServerConfig serverConfig = new NetworkServerConfig();
    
    serverConfig.setServiceName(_serviceName);
    serverConfig.setZooKeeperConnectString(_zooKeeperURL);
    serverConfig.setZooKeeperSessionTimeoutMillis(_zooKeeperSessionTimeout);
    serverConfig.setRequestThreadCorePoolSize(_requestThreadCorePoolSize);
    serverConfig.setRequestThreadMaxPoolSize(_requestThreadMaxPoolSize);
    serverConfig.setRequestThreadKeepAliveTimeSecs(_requestThreadKeepAliveTimeSecs);
    serverConfig.setClusterClient(_clusterClient);
    return serverConfig;
  }
}
