/**
 * 
 */
package com.senseidb.cluster.client;

import java.util.Set;
import java.util.concurrent.Future;

import com.google.protobuf.Message;
import com.linkedin.norbert.cluster.ClusterDisconnectedException;
import com.linkedin.norbert.cluster.InvalidClusterException;
import com.linkedin.norbert.cluster.InvalidNodeException;
import com.linkedin.norbert.javacompat.cluster.Node;
import com.linkedin.norbert.javacompat.network.*;
import com.linkedin.norbert.javacompat.network.IntegerConsistentHashPartitionedLoadBalancerFactory;
import com.linkedin.norbert.javacompat.network.NettyPartitionedNetworkClient;
import com.linkedin.norbert.network.NoNodesAvailableException;
import com.linkedin.norbert.network.ResponseIterator;
import com.linkedin.norbert.network.Serializer;

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
      _networkClient = new NettyPartitionedNetworkClient<Integer>(netConfig, new IntegerConsistentHashPartitionedLoadBalancerFactory(1, false));
    }
  }

    @Override
    public <RequestMsg, ResponseMsg> Future<ResponseMsg> sendRequest(Integer partition, RequestMsg request, Serializer<RequestMsg, ResponseMsg> serializer) throws InvalidClusterException, NoNodesAvailableException, ClusterDisconnectedException {
        return _networkClient.sendRequest(partition, request, serializer);
    }

    @Override
    public <RequestMsg, ResponseMsg> ResponseIterator<ResponseMsg> sendRequest(Set<Integer> partitions, RequestMsg request, Serializer<RequestMsg, ResponseMsg> serializer) throws InvalidClusterException, NoNodesAvailableException, ClusterDisconnectedException {
        return _networkClient.sendRequest(partitions, request, serializer);
    }

    @Override
    public <RequestMsg, ResponseMsg> ResponseIterator<ResponseMsg> sendRequest(Set<Integer> partitions, RequestBuilder<Integer, RequestMsg> requestBuilder, Serializer<RequestMsg, ResponseMsg> serializer) throws Exception {
        return _networkClient.sendRequest(partitions, requestBuilder, serializer);
    }

    @Override
    public <RequestMsg, ResponseMsg, T> T sendRequest(Set<Integer> partitions, RequestBuilder<Integer, RequestMsg> requestBuilder, ScatterGatherHandler<RequestMsg, ResponseMsg, T, Integer> scatterGatherHandler, Serializer<RequestMsg, ResponseMsg> serializer) throws Exception {
        return _networkClient.sendRequest(partitions, requestBuilder, scatterGatherHandler, serializer);

    }

    @Override
    public <RequestMsg, ResponseMsg> ResponseIterator<ResponseMsg> sendRequestToOneReplica(Integer partitionedId, RequestBuilder<Integer, RequestMsg> requestBuilder, Serializer<RequestMsg, ResponseMsg> serializer) throws InvalidClusterException, NoNodesAvailableException, ClusterDisconnectedException {
      return _networkClient.sendRequestToOneReplica(partitionedId, requestBuilder, serializer);
    }

    @Override
    public <RequestMsg, ResponseMsg> ResponseIterator<ResponseMsg> sendRequestToOneReplica(Integer partitionedId, RequestMsg request, Serializer<RequestMsg, ResponseMsg> serializer) throws InvalidClusterException, NoNodesAvailableException, ClusterDisconnectedException {
      return _networkClient.sendRequestToOneReplica(partitionedId, request, serializer);
    }

    @Override
    public <RequestMsg, ResponseMsg> Future<ResponseMsg> sendRequestToNode(RequestMsg request, Node node, Serializer<RequestMsg, ResponseMsg> serializer) throws InvalidNodeException, ClusterDisconnectedException {
        return _networkClient.sendRequestToNode(request, node, serializer);
    }

    @Override
    public <RequestMsg, ResponseMsg> ResponseIterator<ResponseMsg> broadcastMessage(RequestMsg request, Serializer<RequestMsg, ResponseMsg> serializer) throws ClusterDisconnectedException {
        return _networkClient.broadcastMessage(request, serializer);
    }

  public void shutdown()
  {
    _networkClient.shutdown();
  }

}
