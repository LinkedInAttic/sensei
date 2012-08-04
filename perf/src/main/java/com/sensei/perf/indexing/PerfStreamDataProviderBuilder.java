package com.sensei.perf.indexing;

import java.io.File;
import java.util.Comparator;
import java.util.Set;

import org.json.JSONObject;

import com.linkedin.zoie.impl.indexing.StreamDataProvider;

import com.senseidb.gateway.SenseiGateway;
import com.senseidb.indexing.DataSourceFilter;
import com.senseidb.indexing.ShardingStrategy;
import com.linkedin.zoie.impl.indexing.ZoieConfig;

public class PerfStreamDataProviderBuilder extends SenseiGateway<JSONObject> {

        private final Comparator<String> _versionComparator = ZoieConfig.DEFAULT_VERSION_COMPARATOR;
          
        public PerfStreamDataProviderBuilder(){
        }

        @Override
        public StreamDataProvider<JSONObject> buildDataProvider(DataSourceFilter<JSONObject> dataFilter, String oldSinceKey, 
           ShardingStrategy shardingStrategy, Set<Integer> partitions) throws Exception {
                String file = config.get("file");
                int maxIter = Integer.parseInt(config.get("maxIter"));
                
                PerfVersion version = PerfVersion.parse(oldSinceKey);
                PerfStreamDataProvider provider = new PerfStreamDataProvider(new File(file), version, maxIter);

                PerfJsonFilter perfFilter = new PerfJsonFilter(maxIter);
                provider.setFilter(perfFilter);
                return provider;
        }

          @Override
          public Comparator<String> getVersionComparator() {
            return _versionComparator;
          }
}
