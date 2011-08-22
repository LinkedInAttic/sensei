package com.sensei.search.cluster.routing;

import java.util.List;

import scala.actors.threadpool.Arrays;

import com.linkedin.norbert.javacompat.cluster.Node;

public class RoutingInfo
{
  public final int[] partitions;
  public final int[] nodegroup;
  public final List<Node>[] nodelist;

  public RoutingInfo(final List<Node>[] nodelist, int[] partitions, int[] nodegroup)
  {
    this.partitions = partitions;
    this.nodelist = nodelist;
    this.nodegroup = nodegroup;
  }

  public String toString()
  {
    StringBuffer sb = new StringBuffer();
    sb.append("Nodes: ").append(Arrays.toString(nodegroup)).append(" each for partitions: ").append(Arrays.toString(partitions));
    return sb.toString();
  }
}
