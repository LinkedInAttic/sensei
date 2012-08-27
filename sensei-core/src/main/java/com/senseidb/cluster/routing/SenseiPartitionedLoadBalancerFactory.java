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
