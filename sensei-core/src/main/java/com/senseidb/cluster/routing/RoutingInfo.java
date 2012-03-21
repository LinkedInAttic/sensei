package com.senseidb.cluster.routing;

import java.util.Arrays;
import java.util.List;

import com.linkedin.norbert.javacompat.cluster.Node;

public class RoutingInfo
{
  // Three parallel arrays

  /** Index to partition Id */
  public final int[] partitions;

  /** Which node to use in each partition */
  public final int[] nodegroup;

  /** List of nodes for each partition */
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
//
//      StringBuilder builder = new StringBuilder("Nodes: ");
//      for (int i = 0; i < partitions.length; i++) {
//          builder.append(String.format("p%d:%d", partitions[i], nodelist[i].get(nodegroup[i]).getId())).append(" ");
//      }
//
//      return builder.toString();
  }
}
