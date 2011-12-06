package com.sensei.indexing.api.gateway;

import java.util.Comparator;

import org.apache.commons.configuration.Configuration;
import org.json.JSONObject;
import org.springframework.context.ApplicationContext;

import proj.zoie.impl.indexing.StreamDataProvider;

import com.sensei.conf.SenseiSchema;
import com.sensei.indexing.api.DataSourceFilter;

public abstract class SenseiGateway<V>{
	abstract public String getName();
	protected Configuration _conf;
	
	public SenseiGateway(Configuration conf){
	  _conf = conf;
	}
	
	final public DataSourceFilter<V> getDataSourceFilter(SenseiSchema senseiSchema,ApplicationContext pluginCtx){
		String filter = _conf.getString("filter",null);
		if (filter!=null){
			DataSourceFilter<V> dataSourceFilter = (DataSourceFilter<V>)pluginCtx.getBean(filter);
    dataSourceFilter.setSrcDataStore(senseiSchema.getSrcDataStore());
    dataSourceFilter.setSrcDataField(senseiSchema.getSrcDataField());
    return dataSourceFilter;
		}
		return null;
	}
	
	final public StreamDataProvider<JSONObject> buildDataProvider(SenseiSchema senseiSchema,String oldSinceKey,ApplicationContext plugin) throws Exception{
		DataSourceFilter<V> filter = getDataSourceFilter(senseiSchema,plugin);
		return buildDataProvider(filter,oldSinceKey,plugin);
	}
	
	abstract public StreamDataProvider<JSONObject> buildDataProvider(DataSourceFilter<V> dataFilter,
			String oldSinceKey,ApplicationContext plugin) throws Exception;
	
	abstract public Comparator<String> getVersionComparator();
}