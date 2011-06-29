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
	
	final public DataSourceFilter<V> getDataSourceFilter(Configuration conf,SenseiSchema senseiSchema,ApplicationContext pluginCtx){
		String filter = conf.getString("filter",null);
		if (filter!=null){
			DataSourceFilter<V> dataSourceFilter = (DataSourceFilter<V>)pluginCtx.getBean(filter);
    dataSourceFilter.setSrcDataStore(senseiSchema.getSrcDataStore());
    dataSourceFilter.setSrcDataField(senseiSchema.getSrcDataField());
    return dataSourceFilter;
		}
		return null;
	}
	
	final public StreamDataProvider<JSONObject> buildDataProvider(Configuration conf,SenseiSchema senseiSchema,
    Comparator<String> versionComparator,String oldSinceKey,ApplicationContext plugin) throws Exception{
		DataSourceFilter<V> filter = getDataSourceFilter(conf,senseiSchema,plugin);
		return buildDataProvider(conf,filter,versionComparator,oldSinceKey,plugin);
	}
	
	abstract public StreamDataProvider<JSONObject> buildDataProvider(Configuration conf,DataSourceFilter<V> dataFilter,Comparator<String> versionComparator,
			String oldSinceKey,ApplicationContext plugin) throws Exception;
}