//package com.senseidb.cluster.routing;
//
//import java.util.Set;
//
//import org.apache.log4j.Logger;
//
//import com.linkedin.norbert.javacompat.cluster.Node;
//
//public class RingHashLoadBalancerFactory implements SenseiLoadBalancerFactory
//{
//  private static Logger logger = Logger.getLogger(RingHashLoadBalancerFactory.class);
//  private final int _numberOfReplicas;
//  private final HashProvider _hashingStrategy;
//
//  public RingHashLoadBalancerFactory(HashProvider hashing, int numRep)
//  {
//    _numberOfReplicas = numRep;
//    _hashingStrategy = hashing;
//  }
//
//  @Override
//  public SenseiLoadBalancer newLoadBalancer(Set<Node> nodes)
//  {
//    return new RingHashLoadBalancer(_hashingStrategy, _numberOfReplicas, nodes);
//  }
//}
