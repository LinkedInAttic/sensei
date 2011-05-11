/**
 * 
 */
package com.sensei.search.cluster.client;

import java.util.Set;
import java.util.concurrent.Future;

import com.linkedin.norbert.cluster.ClusterDisconnectedException;
import com.linkedin.norbert.cluster.InvalidClusterException;
import com.linkedin.norbert.cluster.InvalidNodeException;
import com.linkedin.norbert.javacompat.cluster.Node;
import com.linkedin.norbert.javacompat.network.*;
import com.linkedin.norbert.network.NoNodesAvailableException;
import com.linkedin.norbert.network.ResponseIterator;
import com.linkedin.norbert.network.Serializer;
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
      _networkClient = new NettyPartitionedNetworkClient<Integer>(netConfig, new IntegerConsistentHashPartitionedLoadBalancerFactory(1));
    }
  }
  
  public <RequestMsg, ResponseMsg> Future<ResponseMsg> sendRequest(Integer id, RequestMsg request, Serializer<RequestMsg, ResponseMsg> serializer) throws InvalidClusterException, NoNodesAvailableException, ClusterDisconnectedException {
    return _networkClient.sendRequest(id, request, serializer);
  }

  public <RequestMsg, ResponseMsg> ResponseIterator<ResponseMsg> broadcastMessage(RequestMsg request, Serializer<RequestMsg, ResponseMsg> serializer)
          throws ClusterDisconnectedException {
    return _networkClient.broadcastMessage(request, serializer);
  }

  public <RequestMsg, ResponseMsg> Future<ResponseMsg> sendRequestToNode(RequestMsg request, Node node, Serializer<RequestMsg, ResponseMsg> serializer) throws InvalidNodeException, ClusterDisconnectedException {
    return _networkClient.sendRequestToNode(request, node, serializer);
  }

  @Override
  public <RequestMsg, ResponseMsg> ResponseIterator<ResponseMsg> sendRequest(Set<Integer> partitionIds, RequestBuilder<Integer, RequestMsg> requestBuilder, Serializer<RequestMsg, ResponseMsg> serializer) throws Exception {
    return _networkClient.sendRequest(partitionIds, requestBuilder, serializer);
  }

  @Override
  public <RequestMsg, ResponseMsg> ResponseIterator<ResponseMsg> sendRequestToOneReplica(RequestBuilder<Integer, RequestMsg> requestBuilder, Serializer<RequestMsg, ResponseMsg> serializer) throws InvalidClusterException, NoNodesAvailableException, ClusterDisconnectedException {
    return _networkClient.sendRequestToOneReplica(requestBuilder, serializer);
  }

  public void shutdown()
  {
    _networkClient.shutdown();
  }
}
