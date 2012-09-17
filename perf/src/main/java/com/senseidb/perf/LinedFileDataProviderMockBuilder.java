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

package com.senseidb.perf;

import java.io.File;
import java.util.Comparator;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

import org.json.JSONObject;

import proj.zoie.impl.indexing.StreamDataProvider;
import proj.zoie.impl.indexing.ZoieConfig;

import com.senseidb.gateway.SenseiGateway;
import com.senseidb.indexing.DataSourceFilter;
import com.senseidb.indexing.ShardingStrategy;

public class LinedFileDataProviderMockBuilder  extends SenseiGateway<String>{

  private Comparator<String> _versionComparator = ZoieConfig.DEFAULT_VERSION_COMPARATOR;


  @Override
  public StreamDataProvider<JSONObject> buildDataProvider(DataSourceFilter<String> dataFilter,
      String oldSinceKey,
      ShardingStrategy shardingStrategy,
      Set<Integer> partitions) throws Exception
  {

    String path = config.get("file.path");
    if (path == null) {
      path = "data/cars.json";
    }
    long offset = oldSinceKey == null ? 0L : Long.parseLong(oldSinceKey);
    

    PerfFileDataProvider provider = new PerfFileDataProvider(_versionComparator, new File(path), 0L, new LinkedBlockingQueue<JSONObject>(30000));
    if (dataFilter!=null){
      provider.setFilter(dataFilter);
    }
    return provider;
  }

  @Override
  public Comparator<String> getVersionComparator() {
    return _versionComparator;
  }
}
