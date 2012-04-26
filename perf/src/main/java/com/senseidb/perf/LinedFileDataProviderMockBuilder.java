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