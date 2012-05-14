package com.senseidb.example.tweets.gateway;

import java.util.Comparator;
import java.util.Set;

import org.json.JSONObject;

import com.linkedin.zoie.impl.indexing.StreamDataProvider;

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
