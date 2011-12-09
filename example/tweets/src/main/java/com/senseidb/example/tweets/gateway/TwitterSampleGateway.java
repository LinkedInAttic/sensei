package com.senseidb.example.tweets.gateway;

import java.util.Comparator;

import org.apache.commons.configuration.Configuration;
import org.json.JSONObject;

import proj.zoie.impl.indexing.StreamDataProvider;

import com.sensei.indexing.api.DataSourceFilter;
import com.sensei.indexing.api.gateway.SenseiGateway;

public class TwitterSampleGateway extends SenseiGateway<JSONObject> {

  public TwitterSampleGateway(Configuration conf) {
    super(conf);
  }

  @Override
  public StreamDataProvider<JSONObject> buildDataProvider(
      DataSourceFilter<JSONObject> dataFilter, String oldSinceKey) throws Exception {
    return new TwitterSampleStreamer(_conf, SenseiGateway.DEFAULT_VERSION_COMPARATOR);
  }

  @Override
  public Comparator<String> getVersionComparator() {
    return SenseiGateway.DEFAULT_VERSION_COMPARATOR;
  }
}
