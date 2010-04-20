/**
 * 
 */
package com.sensei.search.cluster.client;

import java.io.File;
import java.util.List;
import java.util.concurrent.Future;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;

import com.google.protobuf.Message;
import com.linkedin.norbert.cluster.ClusterDisconnectedException;
import com.linkedin.norbert.cluster.InvalidClusterException;
import com.linkedin.norbert.cluster.InvalidNodeException;
import com.linkedin.norbert.cluster.Node;
import com.linkedin.norbert.cluster.javaapi.ClusterClient;
import com.linkedin.norbert.network.NoNodesAvailableException;
import com.linkedin.norbert.network.ResponseIterator;
import com.linkedin.norbert.network.javaapi.NettyPartitionedNetworkClient;
import com.linkedin.norbert.network.javaapi.NetworkClientConfig;
import com.linkedin.norbert.network.javaapi.PartitionedLoadBalancerFactory;
import com.linkedin.norbert.network.javaapi.PartitionedNetworkClient;
import com.linkedin.norbert.network.javaapi.ScatterGatherHandler;
import com.sensei.search.cluster.routing.UniformPartitionedRoutingFactory;
import com.sensei.search.nodes.SenseiNetworkClientConfig;
import com.sensei.search.util.SenseiDefaults;

/**
 * @author nnarkhed
 *
 */
public class SenseiNetworkClient implements PartitionedNetworkClient<Integer>
{
  private PartitionedNetworkClient<Integer> _networkClient;

  public SenseiNetworkClient(ClusterClient clusterClient, PartitionedLoadBalancerFactory<Integer> routerFactory)
  {
    String confDirName=System.getProperty("conf.dir");
    File confDir = null;
    if (confDirName == null)
    {
      confDir = new File("node-conf");
    }
    else
    {
      confDir = new File(confDirName);
    }

    File confFile = new File(confDir, SenseiDefaults.SENSEI_CLUSTER_CONF_FILE);
    
    ApplicationContext springCtx = new FileSystemXmlApplicationContext("file:"+confFile.getAbsolutePath());
    SenseiNetworkClientConfig config = (SenseiNetworkClientConfig)springCtx.getBean("network-client-config");
    
    NetworkClientConfig netConfig = config.getNetworkConfigObject();
    netConfig.setClusterClient(clusterClient);

    if(routerFactory != null)
    {
      _networkClient = new NettyPartitionedNetworkClient<Integer>(netConfig, routerFactory);
    }
    else
    {
      _networkClient = new NettyPartitionedNetworkClient<Integer>(netConfig, 
          new UniformPartitionedRoutingFactory());
    }
  }

  public SenseiNetworkClient(File confFile, ClusterClient clusterClient, PartitionedLoadBalancerFactory<Integer> routerFactory)
  {
    ApplicationContext springCtx = new FileSystemXmlApplicationContext("file:"+confFile.getAbsolutePath());
    SenseiNetworkClientConfig config = (SenseiNetworkClientConfig)springCtx.getBean("network-client-config");
    
    NetworkClientConfig netConfig = config.getNetworkConfigObject();
    netConfig.setClusterClient(clusterClient);
    
    if(routerFactory != null)
    {
      _networkClient = new NettyPartitionedNetworkClient<Integer>(netConfig, routerFactory);
    }
    else
    {
      _networkClient = new NettyPartitionedNetworkClient<Integer>(netConfig, 
          new UniformPartitionedRoutingFactory());
    }
  }
  
//  public PartitionedNetworkClient<Integer> getNetworkClient()
//  {
//    return _networkClient;
//  }
  
  public Future<Message> sendMessage(Integer id, Message message) throws InvalidClusterException,
      NoNodesAvailableException,
      ClusterDisconnectedException
  {
    return _networkClient.sendMessage(id, message);
  }

  public ResponseIterator sendMessage(List<Integer> ids, Message message) throws InvalidClusterException,
      NoNodesAvailableException,
      ClusterDisconnectedException
  {
    return _networkClient.sendMessage(ids, message);
  }

  public <T> T sendMessage(List<Integer> ids,
                           Message message,
                           ScatterGatherHandler<T, Integer> scatterGather) throws Exception
  {
//    return _networkClient.sendMessage(ids, message, scatterGather);
    return null;
  }

  public ResponseIterator broadcastMessage(Message message) throws ClusterDisconnectedException
  {
    return _networkClient.broadcastMessage(message);
  }

  public void registerRequest(Message request, Message response)
  {
    _networkClient.registerRequest(request, response);
  }

  public Future<Message> sendMessageToNode(Message message, Node node) throws InvalidNodeException,
      ClusterDisconnectedException
  {
    return _networkClient.sendMessageToNode(message, node);
  }

  public void shutdown()
  {
    _networkClient.shutdown();
  }

}
