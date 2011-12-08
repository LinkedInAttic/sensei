package com.sensei.indexing.api.gateway;

import java.util.Comparator;

import org.apache.commons.configuration.Configuration;
import org.json.JSONObject;

import proj.zoie.impl.indexing.StreamDataProvider;

import com.sensei.conf.SenseiSchema;
import com.sensei.indexing.api.DataSourceFilter;
import com.sensei.indexing.api.DefaultStreamingIndexingManager;
import com.sensei.plugin.SenseiPluginRegistry;

public abstract class SenseiGateway<V>{
	abstract public String getName();
	protected Configuration _conf;
  protected SenseiPluginRegistry pluginRegistry;

	public SenseiGateway(Configuration conf){
	  _conf = conf;
    this.pluginRegistry = SenseiPluginRegistry.build(conf);
	}

	final public DataSourceFilter<V> getDataSourceFilter(SenseiSchema senseiSchema, SenseiPluginRegistry pluginRegistry){
		DataSourceFilter<V> dataSourceFilter = pluginRegistry.getBeanByFullPrefix(DefaultStreamingIndexingManager.CONFIG_PREFIX + ".filter", DataSourceFilter.class);
    if (dataSourceFilter != null) {
		dataSourceFilter.setSrcDataStore(senseiSchema.getSrcDataStore());
    dataSourceFilter.setSrcDataField(senseiSchema.getSrcDataField());
    return dataSourceFilter;
		}
		return null;
	}

	final public StreamDataProvider<JSONObject> buildDataProvider(SenseiSchema senseiSchema,String oldSinceKey, SenseiPluginRegistry pluginRegistry) throws Exception{
		DataSourceFilter<V> filter = getDataSourceFilter(senseiSchema,pluginRegistry);
		return buildDataProvider(filter,oldSinceKey);
	}

	abstract public StreamDataProvider<JSONObject> buildDataProvider(DataSourceFilter<V> dataFilter,
			String oldSinceKey) throws Exception;

	abstract public Comparator<String> getVersionComparator();
}