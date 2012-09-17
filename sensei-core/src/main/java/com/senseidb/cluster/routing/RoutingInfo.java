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
  }
}
