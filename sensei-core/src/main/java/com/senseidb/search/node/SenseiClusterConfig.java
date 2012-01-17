package com.senseidb.search.node;

public class SenseiClusterConfig
{
  private static final String DEFAULT_ZK_URL = "localhost:2181";
  
  private String _clusterName;
  private String _zooKeeperURL;
  private int zooKeeperSessionTimeoutMillis;
  
   public void setClusterName(String clusterName)
   {
     _clusterName = clusterName;
   }
  
   public void setZooKeeperSessionTimeoutMillis(int zooKeeperSessionTimeout)
   {
     zooKeeperSessionTimeoutMillis = zooKeeperSessionTimeout;
   }
   
   public int getZooKeeperSessionTimeoutMillis()
   {
     return zooKeeperSessionTimeoutMillis;
   }
   
   public String getClusterName()
   {
     return _clusterName;
   }
   
   public void setZooKeeperURL(String zookeeperURL)
   {
     _zooKeeperURL = zookeeperURL;
   }
   
   public String getZooKeeperURL()
   {
     return (_zooKeeperURL != null ? _zooKeeperURL : DEFAULT_ZK_URL);
   }
}
