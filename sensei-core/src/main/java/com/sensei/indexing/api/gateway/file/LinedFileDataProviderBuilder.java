package com.sensei.indexing.api.gateway.file;

import java.io.File;
import java.util.Comparator;

import org.apache.commons.configuration.Configuration;
import org.json.JSONObject;
import org.springframework.context.ApplicationContext;

import proj.zoie.impl.indexing.StreamDataProvider;

import com.sensei.dataprovider.file.LinedJsonFileDataProvider;
import com.sensei.indexing.api.DataSourceFilter;
import com.sensei.indexing.api.gateway.SenseiGateway;

public  class LinedFileDataProviderBuilder extends SenseiGateway<String>{

	public static final String name = "file";
	
	@Override
	public StreamDataProvider<JSONObject> buildDataProvider(
			Configuration conf,DataSourceFilter<String> dataFilter,Comparator<String> versionComparator,
			String oldSinceKey,ApplicationContext plugin) throws Exception{
		String path = conf.getString("path");
		long offset = oldSinceKey == null ? 0L : Long.parseLong(oldSinceKey);
		
		
		LinedJsonFileDataProvider provider = new LinedJsonFileDataProvider(versionComparator, new File(path), offset);
		if (dataFilter!=null){
		  provider.setFilter(dataFilter);
		}
		return provider;
	}
	
	@Override
	public String getName() {
		return name;
	}
}
