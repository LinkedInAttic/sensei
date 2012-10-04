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

package com.senseidb.example.tweets.gateway;

import java.util.Comparator;
import java.util.Set;

import org.json.JSONObject;

import proj.zoie.impl.indexing.StreamDataProvider;

import com.senseidb.gateway.SenseiGateway;
import com.senseidb.indexing.DataSourceFilter;
import com.senseidb.indexing.ShardingStrategy;

public class TwitterSampleGateway extends SenseiGateway<JSONObject> {



  @Override

  public StreamDataProvider<JSONObject> buildDataProvider(DataSourceFilter<JSONObject> dataFilter,
                                                          String oldSinceKey,
                                                          ShardingStrategy shardingStrategy,
                                                          Set<Integer> partitions) throws Exception{
    return new TwitterSampleStreamer(config, SenseiGateway.DEFAULT_VERSION_COMPARATOR);

  }

  @Override
  public Comparator<String> getVersionComparator() {
    return SenseiGateway.DEFAULT_VERSION_COMPARATOR;
  }
}
