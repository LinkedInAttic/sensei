package com.sensei.search.nodes.impl;

import com.linkedin.norbert.javacompat.cluster.ClusterClient;
import com.linkedin.norbert.javacompat.network.NettyNetworkServer;
import com.linkedin.norbert.javacompat.network.NetworkServer;
import com.linkedin.norbert.javacompat.network.NetworkServerConfig;

public class SenseiBuilderHelper {
  private SenseiBuilderHelper(){}
  
  public static NetworkServer buildDefaultNetworkServer(ClusterClient clusterClient){
	  NetworkServerConfig serverConfig = new NetworkServerConfig();
	  serverConfig.setClusterClient(clusterClient);
	  serverConfig.setRequestThreadCorePoolSize(5);
	  serverConfig.setRequestThreadKeepAliveTimeSecs(300);
	  serverConfig.setRequestThreadMaxPoolSize(10);
	  return new NettyNetworkServer(serverConfig);
  }
}
