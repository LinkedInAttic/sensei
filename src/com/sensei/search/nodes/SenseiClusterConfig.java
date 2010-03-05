package com.sensei.search.nodes;

public class SenseiClusterConfig
{
  private static final String DEFAULT_ZK_URL = "localhost:2181";
  
  private String _clusterName;
  private String _zookeeperURL;
   
   public void setClusterName(String clusterName)
   {
     _clusterName = clusterName;
   }
   
   public String getClusterName()
   {
     return _clusterName;
   }
   
   public void setZooKeeperURL(String zookeeperURL)
   {
     _zookeeperURL = zookeeperURL;
   }
   
   public String getZooKeeperURL()
   {
     return (_zookeeperURL != null ? _zookeeperURL : DEFAULT_ZK_URL);
   }
}
