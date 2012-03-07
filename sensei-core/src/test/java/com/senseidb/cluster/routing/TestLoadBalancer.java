package com.senseidb.cluster.routing;

import com.linkedin.norbert.javacompat.cluster.Node;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;


public class TestLoadBalancer
{
  private static final class MockNode implements Node
  {
    private final int id;
    private final String url;
    private final Set<Integer> partitionIds;
    private final boolean available;

    private MockNode(String url, int id, int... partIds)
    {
      this.available = true;
      this.url = url;
      this.id = id;

      Set<Integer> set = new HashSet<Integer>();
      for (int partId : partIds)
      {
        set.add(partId);
      }
      partitionIds = Collections.unmodifiableSet(set);
    }

    @Override
    public int getId()
    {
      return id;
    }

    @Override
    public String getUrl()
    {
      return url;
    }

    @Override
    public Set<Integer> getPartitionIds()
    {
      return partitionIds;
    }

    @Override
    public boolean isAvailable()
    {
      return available;
    }

    @Override
    public String toString()
    {
      return String.format("%d:%s", id, partitionIds);
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      MockNode mockNode = (MockNode) o;

      if (available != mockNode.available) return false;
      if (id != mockNode.id) return false;
      if (!partitionIds.equals(mockNode.partitionIds)) return false;
      if (!url.equals(mockNode.url)) return false;

      return true;
    }

    @Override
    public int hashCode()
    {
      int result = id;
      result = 31 * result + url.hashCode();
      result = 31 * result + partitionIds.hashCode();
      result = 31 * result + (available ? 1 : 0);
      return result;
    }
  }

  // See if same key always route back to same set of nodes
  @Test
  public void testConsistentHash()
  {
    Set<Node> nodes = new HashSet<Node>();

    // 3 nodes for p1, 3 nodes for p2, 4 nodes for p3
    nodes.add(new MockNode("url1", 1, 1, 2, 3));
    nodes.add(new MockNode("url2", 2, 2, 3));
    nodes.add(new MockNode("url3", 3, 1, 3));
    nodes.add(new MockNode("url4", 4, 1));
    nodes.add(new MockNode("url5", 5, 2, 3));

    SenseiLoadBalancerFactory factory = new ConsistentHashLoadBalancerFactory(new MD5HashProvider(), 1000);
    SenseiLoadBalancer loadBalancer = factory.newLoadBalancer(nodes);

    // All partitions should route to the same node on multiple calls
    Assert.assertEquals(getRoutingMap(loadBalancer.route("param")), getRoutingMap(loadBalancer.route("param")));
  }

  @Test
  public void testConsistentHashRemove()
  {
    Set<Node> nodes = new HashSet<Node>();

    // 3 nodes for p1, 3 nodes for p2, 4 nodes for p3
    nodes.add(new MockNode("url1", 1, 1, 2, 3));
    nodes.add(new MockNode("url2", 2, 2, 3));
    nodes.add(new MockNode("url3", 3, 1, 3));
    nodes.add(new MockNode("url4", 4, 1));
    nodes.add(new MockNode("url5", 5, 2, 3));

    SenseiLoadBalancerFactory factory = new ConsistentHashLoadBalancerFactory(new MD5HashProvider(), 1000);
    SenseiLoadBalancer loadBalancer = factory.newLoadBalancer(nodes);

    Map<String, Map<Integer, Integer>> routes = new HashMap<String, Map<Integer, Integer>>();
    int iteration = 1000;
    for (int i = 0; i < iteration; i++)
    {
      String key = "param" + i;
      routes.put(key, getRoutingMap(loadBalancer.route(key)));
    }

    // Remove node 4
    nodes.remove(new MockNode("url4", 4, 1));
    loadBalancer = factory.newLoadBalancer(nodes);

    // For all keys, routing for 2 and 3 should state the same, while about 1/3 of partition changed
    int changed = 0;
    Map<Integer, Integer> changes = new HashMap<Integer, Integer>();
    for (int i = 0; i < iteration; i++)
    {
      String key = "param" + i;
      Map<Integer, Integer> routingMap = getRoutingMap(loadBalancer.route(key));

      Assert.assertEquals(routes.get(key).get(2), routingMap.get(2));
      Assert.assertEquals(routes.get(key).get(3), routingMap.get(3));

      if (routes.get(key).get(1) != routingMap.get(1))
      {
        changed++;
        Integer v = changes.get(routingMap.get(1));
        changes.put(routingMap.get(1), v == null ? 1 : v + 1);
      }
    }

    // Percentage of changed node would be about 1/3 (error within 5 %)
    Assert.assertTrue(Math.abs((double) 1 / 3 - (double) changed / iteration) < iteration * 0.05d);

    // Differences between redistributed bucket should be small
    Assert.assertTrue(sd(changes.values(), changed) < changed * 0.05d);
  }

  // See if the routing is evenly distributed
  @Test
  public void testDistribution()
  {
    Set<Node> nodes = new HashSet<Node>();

    // 3 nodes for p1, 3 nodes for p2, 4 nodes for p3
    nodes.add(new MockNode("url1", 1, 1, 2, 3));
    nodes.add(new MockNode("url2", 2, 2, 3));
    nodes.add(new MockNode("url3", 3, 1, 3));
    nodes.add(new MockNode("url4", 4, 1));
    nodes.add(new MockNode("url5", 5, 2, 3));

    SenseiLoadBalancerFactory factory = new ConsistentHashLoadBalancerFactory(new MD5HashProvider(), 1000);
    SenseiLoadBalancer loadBalancer = factory.newLoadBalancer(nodes);

    // Gather distribution for 3000 keys
    // For each partition, record number of times a node get routed to.
    int iteration = 10000;
    Map<Integer, Map<Integer, Integer>> stats = getDistribution(loadBalancer, iteration);

    for (Map.Entry<Integer, Map<Integer, Integer>> entry : stats.entrySet())
    {
      double diff = (double) iteration / entry.getValue().size() * 0.05;   // Allow 5 percent differences
      Assert.assertTrue(String.format("Discrepancy too big: p=%d, s=%s", entry.getKey(), entry.getValue()),
                        sd(entry.getValue().values(), iteration) < diff);
    }
  }

  // When bucket count change, no. of nodes get changed should be very small
  @Test
  public void testChangeBucketCount() {
    Set<Node> nodes = new HashSet<Node>();

    // 3 nodes for p1, 3 nodes for p2, 4 nodes for p3
    nodes.add(new MockNode("url1", 1, 1, 2, 3));
    nodes.add(new MockNode("url2", 2, 2, 3));
    nodes.add(new MockNode("url3", 3, 1, 3));
    nodes.add(new MockNode("url4", 4, 1));
    nodes.add(new MockNode("url5", 5, 2, 3));

    // Bucket count = 1000
    int bucketCount = 1000;
    SenseiLoadBalancerFactory factory = new ConsistentHashLoadBalancerFactory(new MD5HashProvider(), bucketCount);
    SenseiLoadBalancer loadBalancer = factory.newLoadBalancer(nodes);

    Map<String, Map<Integer, Integer>> routes = new HashMap<String, Map<Integer, Integer>>();
    int iteration = 10000;
    for (int i = 0; i < iteration; i++)
    {
      String key = "param" + i;
      routes.put(key, getRoutingMap(loadBalancer.route(key)));
    }

    // Bucket count = 1005
    int delta = 5;
    int changed = 0;
    factory = new ConsistentHashLoadBalancerFactory(new MD5HashProvider(), bucketCount + delta);
    loadBalancer = factory.newLoadBalancer(nodes);
    for (int i = 0; i < iteration; i++) {
      String key = "param" + i;
      Map<Integer, Integer> routingMap = getRoutingMap(loadBalancer.route(key));

      // Look for changes
      Map<Integer, Integer> oldRoutingMap = routes.get(key);
      for (Map.Entry<Integer, Integer> entry : routingMap.entrySet()) {
        if (!entry.getValue().equals(oldRoutingMap.get(entry.getKey()))) {
          changed++;
        }
      }
    }

    // Total changes should be less than delta/bucketcount ( * 3 since there are three partitions)
    Assert.assertTrue(changed < (double)(iteration * 3 * delta) / bucketCount);
  }

  private Map<Integer, Integer> getRoutingMap(RoutingInfo info)
  {
    Map<Integer, Integer> route = new HashMap<Integer, Integer>();
    for (int i = 0; i < info.partitions.length; i++)
    {
      route.put(info.partitions[i], info.nodelist[i].get(info.nodegroup[i]).getId());
    }
    return route;
  }

  // Find the Standard Deviation
  private double sd(Collection<Integer> numbers, int total)
  {
    int size = numbers.size();
    double avg = (double) total / size;
    int sumOfSq = 0;
    for (Integer number : numbers)
    {
      sumOfSq += (number - avg) * (number - avg);
    }

    return Math.sqrt((double) sumOfSq / size);
  }

  private Map<Integer, Map<Integer, Integer>> getDistribution(SenseiLoadBalancer loadBalancer, int iteration)
  {
    Map<Integer, Map<Integer, Integer>> stats = new HashMap<Integer, Map<Integer, Integer>>();
    for (int i = 0; i < iteration; i++)
    {
      RoutingInfo info = loadBalancer.route("param" + i);
      for (int idx = 0; idx < info.partitions.length; idx++)
      {
        Map<Integer, Integer> stat = stats.get(info.partitions[idx]);
        if (stat == null)
        {
          stat = new HashMap<Integer, Integer>();
          stats.put(info.partitions[idx], stat);
        }
        Integer nodeId = info.nodelist[idx].get(info.nodegroup[idx]).getId();
        Integer count = stat.get(nodeId);
        stat.put(nodeId, count == null ? 1 : count + 1);
      }
    }
    return stats;
  }
}
