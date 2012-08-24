package com.senseidb.cluster.routing;

import com.linkedin.norbert.cluster.InvalidClusterException;
import com.linkedin.norbert.javacompat.cluster.Node;
import com.linkedin.norbert.javacompat.network.*;

import java.util.Map;
import java.util.Set;

public class SenseiPartitionedLoadBalancerFactory implements PartitionedLoadBalancerFactory<String> {
  private final ConsistentHashPartitionedLoadBalancerFactory<String> lbf;

  public SenseiPartitionedLoadBalancerFactory(int bucketCount) {
    HashFunction.MD5HashFunction hashFn = new HashFunction.MD5HashFunction();

    MultiRingConsistentHashPartitionedLoadBalancerFactory<String> fallThroughLbf =
        new MultiRingConsistentHashPartitionedLoadBalancerFactory<String>(
          -1,
          bucketCount,
          hashFn,
          hashFn,
          true);

    this.lbf = new ConsistentHashPartitionedLoadBalancerFactory<String>(bucketCount, hashFn, fallThroughLbf);
  }

  @Override
  public PartitionedLoadBalancer<String> newLoadBalancer(Set<Endpoint> endpoints) throws InvalidClusterException {
    return lbf.newLoadBalancer(endpoints);
  }

  @Override
  public Integer getNumPartitions(Set<Endpoint> endpoints) {
    return lbf.getNumPartitions(endpoints);
  }
}
