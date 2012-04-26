package com.sensei.perf.indexing;

import java.io.File;
import java.util.Comparator;

import org.apache.commons.configuration.Configuration;
import org.json.JSONObject;
import org.springframework.context.ApplicationContext;

import com.linkedin.zoie.impl.indexing.StreamDataProvider;

import com.sensei.indexing.api.DataSourceFilter;
import com.sensei.indexing.api.gateway.SenseiGateway;

public class PerfStreamDataProviderBuilder extends SenseiGateway<JSONObject> {

	private final PerfJsonFilter _perfFilter;
	private final Comparator<String> _versionComparator;
	  
	public PerfStreamDataProviderBuilder(PerfJsonFilter perfFilter){
		_perfFilter = perfFilter;
	}
	@Override
	public String getName() {
		return "perf";
	}

	@Override
	public StreamDataProvider<JSONObject> buildDataProvider(Configuration conf,
			DataSourceFilter<JSONObject> dataFilter,
			Comparator<String> versionComparator, String oldSinceKey,
			ApplicationContext plugin) throws Exception {
		String file = conf.getString("file");
		int maxIter = conf.getInt("maxIter");
		
		PerfVersion version = PerfVersion.parse(oldSinceKey);
		PerfStreamDataProvider provider = new PerfStreamDataProvider(new File(file), version, maxIter);
		provider.setFilter(_perfFilter);
		return provider;
	}

	  @Override
	  public Comparator<String> getVersionComparator() {
	    return _versionComparator;
	  }
}
