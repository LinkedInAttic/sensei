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
package com.senseidb.search.node.impl;

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
