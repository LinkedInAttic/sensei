package com.senseidb.cluster.routing;


import com.linkedin.norbert.javacompat.cluster.Node;
import java.util.Set;


public class ConsistentHashLoadBalancerFactory implements SenseiLoadBalancerFactory {
  
  private final int _multiplyFactor;
  private final HashProvider _hashProvider;

  public ConsistentHashLoadBalancerFactory(HashProvider hashProvider, int multiplyFactor)
  {
    _hashProvider = hashProvider;
    _multiplyFactor = multiplyFactor;
  }

  @Override
  public SenseiLoadBalancer newLoadBalancer(Set<Node> nodes)
  {
    return new ConsistentHashLoadBalancer(_hashProvider, _multiplyFactor, nodes);
  }
}
