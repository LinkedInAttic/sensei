/**
 * 
 */
package com.sensei.search.cluster.client;

import java.util.Set;
import java.util.concurrent.Future;

import com.google.protobuf.Message;
import com.linkedin.norbert.cluster.ClusterDisconnectedException;
import com.linkedin.norbert.cluster.InvalidClusterException;
import com.linkedin.norbert.cluster.InvalidNodeException;
import com.linkedin.norbert.cluster.javaapi.Node;
import com.linkedin.norbert.network.NoNodesAvailableException;
import com.linkedin.norbert.network.ResponseIterator;
import com.linkedin.norbert.network.javaapi.NettyPartitionedNetworkClient;
import com.linkedin.norbert.network.javaapi.NetworkClientConfig;
import com.linkedin.norbert.network.javaapi.PartitionedLoadBalancerFactory;
import com.linkedin.norbert.network.javaapi.PartitionedNetworkClient;
import com.linkedin.norbert.network.javaapi.ScatterGatherHandler;
import com.sensei.search.cluster.routing.UniformPartitionedRoutingFactory;

/**
 * @author nnarkhed
 *
 */
public class SenseiNetworkClient implements PartitionedNetworkClient<Integer>
{
  private final PartitionedNetworkClient<Integer> _networkClient;

  public SenseiNetworkClient(NetworkClientConfig netConfig, PartitionedLoadBalancerFactory<Integer> routerFactory)
  {
    if(routerFactory != null)
    {
      _networkClient = new NettyPartitionedNetworkClient<Integer>(netConfig, routerFactory);
    }
    else
    {
      _networkClient = new NettyPartitionedNetworkClient<Integer>(netConfig, new UniformPartitionedRoutingFactory());
    }
  }
  
  public Future<Message> sendMessage(Integer id, Message message) throws InvalidClusterException,
      NoNodesAvailableException,
      ClusterDisconnectedException
  {
    return _networkClient.sendMessage(id, message);
  }

  public ResponseIterator sendMessage(Set<Integer> ids, Message message) throws InvalidClusterException,
      NoNodesAvailableException,
      ClusterDisconnectedException
  {
    return _networkClient.sendMessage(ids, message);
  }

  public <T> T sendMessage(Set<Integer> ids,
                           Message message,
                           ScatterGatherHandler<T, Integer> scatterGather) throws Exception
  {
    return _networkClient.sendMessage(ids, message, scatterGather);
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
