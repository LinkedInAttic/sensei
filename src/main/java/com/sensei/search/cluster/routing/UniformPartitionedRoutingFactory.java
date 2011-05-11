package com.sensei.search.cluster.routing;

import com.linkedin.norbert.cluster.InvalidClusterException;
import com.linkedin.norbert.javacompat.network.Endpoint;
import com.linkedin.norbert.javacompat.network.IntegerConsistentHashPartitionedLoadBalancerFactory;
import com.linkedin.norbert.javacompat.network.PartitionedLoadBalancer;
import com.linkedin.norbert.javacompat.network.PartitionedLoadBalancerFactory;

import java.util.Set;

public class UniformPartitionedRoutingFactory implements PartitionedLoadBalancerFactory<Integer> {

  @Override
  public PartitionedLoadBalancer<Integer> newLoadBalancer(Set<Endpoint> endpoints) throws InvalidClusterException {
    int size = 0;
    for(Endpoint e : endpoints) {
      size += e.getNode().getPartitionIds().size();
    }

    IntegerConsistentHashPartitionedLoadBalancerFactory factory = new IntegerConsistentHashPartitionedLoadBalancerFactory(size, true);
    return factory.newLoadBalancer(endpoints);
  }
}
