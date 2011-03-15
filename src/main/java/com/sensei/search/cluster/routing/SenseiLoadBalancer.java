package com.sensei.search.cluster.routing;

public interface SenseiLoadBalancer
{
  /**
   * @param routeParam
   * @return the routing information for the given routing parameter.
   */
  public RoutingInfo route(String routeParam);
}
