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
import com.senseidb.cluster.routing.SenseiPartitionedLoadBalancerFactory;

/**
 * @author nnarkhed
 *
 */
public class SenseiNetworkClient implements PartitionedNetworkClient<String>
{
  private final PartitionedNetworkClient<String> _networkClient;

  public SenseiNetworkClient(NetworkClientConfig netConfig, PartitionedLoadBalancerFactory<String> routerFactory)
  {
    if(routerFactory != null)
    {
      _networkClient = new NettyPartitionedNetworkClient<String>(netConfig, routerFactory);
    }
    else
    {
      SenseiPartitionedLoadBalancerFactory lbf = new SenseiPartitionedLoadBalancerFactory(50);
      _networkClient = new NettyPartitionedNetworkClient<String>(netConfig, lbf);
    }
  }

    @Override
    public <RequestMsg, ResponseMsg> Future<ResponseMsg> sendRequest(String partitionedId, RequestMsg request, Serializer<RequestMsg, ResponseMsg> serializer) throws InvalidClusterException, NoNodesAvailableException, ClusterDisconnectedException {
        return _networkClient.sendRequest(partitionedId, request, serializer);
    }

    @Override
    public <RequestMsg, ResponseMsg> ResponseIterator<ResponseMsg> sendRequest(Set<String> partitionedIds, RequestMsg request, Serializer<RequestMsg, ResponseMsg> serializer) throws InvalidClusterException, NoNodesAvailableException, ClusterDisconnectedException {
        return _networkClient.sendRequest(partitionedIds, request, serializer);
    }

    @Override
    public <RequestMsg, ResponseMsg> ResponseIterator<ResponseMsg> sendRequest(Set<String> partitionedIds, RequestBuilder<String, RequestMsg> requestBuilder, Serializer<RequestMsg, ResponseMsg> serializer) throws Exception {
        return _networkClient.sendRequest(partitionedIds, requestBuilder, serializer);
    }

    @Override
    public <RequestMsg, ResponseMsg, T> T sendRequest(Set<String> partitionedIds, RequestBuilder<String, RequestMsg> requestBuilder, ScatterGatherHandler<RequestMsg, ResponseMsg, T, String> scatterGatherHandler, Serializer<RequestMsg, ResponseMsg> serializer) throws Exception {
        return _networkClient.sendRequest(partitionedIds, requestBuilder, scatterGatherHandler, serializer);

    }

    @Override
    public <RequestMsg, ResponseMsg> ResponseIterator<ResponseMsg> sendRequestToOneReplica(String partitionedId, RequestBuilder<Integer, RequestMsg> requestBuilder, Serializer<RequestMsg, ResponseMsg> serializer) throws InvalidClusterException, NoNodesAvailableException, ClusterDisconnectedException {
      return _networkClient.sendRequestToOneReplica(partitionedId, requestBuilder, serializer);
    }

    @Override
    public <RequestMsg, ResponseMsg> ResponseIterator<ResponseMsg> sendRequestToOneReplica(String partitionedId, RequestMsg request, Serializer<RequestMsg, ResponseMsg> serializer) throws InvalidClusterException, NoNodesAvailableException, ClusterDisconnectedException {
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

  @Override
  public <RequestMsg, ResponseMsg> ResponseIterator<ResponseMsg> sendRequestToPartitions(String partitionedId, Set<Integer> partitions, RequestBuilder<Integer, RequestMsg> requestBuilder, Serializer<RequestMsg, ResponseMsg> serializer) throws InvalidClusterException, NoNodesAvailableException, ClusterDisconnectedException {
    return _networkClient.sendRequestToPartitions(partitionedId, partitions, requestBuilder, serializer);
  }

  public void shutdown()
  {
    _networkClient.shutdown();
  }

}
