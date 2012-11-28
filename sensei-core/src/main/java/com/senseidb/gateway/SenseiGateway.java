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
package com.senseidb.gateway;

import java.util.Comparator;
import java.util.Set;

import org.json.JSONObject;

import proj.zoie.impl.indexing.StreamDataProvider;
import proj.zoie.impl.indexing.ZoieConfig;

import com.senseidb.conf.SenseiSchema;
import com.senseidb.indexing.DataSourceFilter;
import com.senseidb.indexing.ShardingStrategy;
import com.senseidb.plugin.AbstractSenseiPlugin;
import com.senseidb.plugin.SenseiPluginRegistry;

public abstract class SenseiGateway<V> extends AbstractSenseiPlugin {

  public static Comparator<String> DEFAULT_VERSION_COMPARATOR = ZoieConfig.DEFAULT_VERSION_COMPARATOR;

  final public DataSourceFilter<V> getDataSourceFilter(SenseiSchema senseiSchema) {
    DataSourceFilter<V> dataSourceFilter = pluginRegistry.getBeanByFullPrefix("sensei.gateway.filter",
        DataSourceFilter.class);
    if (dataSourceFilter != null) {
      dataSourceFilter.setSrcDataStore(senseiSchema.getSrcDataStore());
      dataSourceFilter.setSrcDataField(senseiSchema.getSrcDataField());
      return dataSourceFilter;
    }
    return null;
  }

  final public StreamDataProvider<JSONObject> buildDataProvider(SenseiSchema senseiSchema, String oldSinceKey,
	  ShardingStrategy shardingStrategy, Set<Integer> partitions) throws Exception {
    DataSourceFilter<V> filter = getDataSourceFilter(senseiSchema);
    return buildDataProvider(filter, oldSinceKey, shardingStrategy, partitions);
  }

  abstract public StreamDataProvider<JSONObject> buildDataProvider(DataSourceFilter<V> dataFilter, String oldSinceKey,
      ShardingStrategy shardingStrategy, Set<Integer> partitions) throws Exception;

  abstract public Comparator<String> getVersionComparator();
}
