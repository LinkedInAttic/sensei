package com.sensei.perf.indexing;

import org.json.JSONObject;

import com.sensei.indexing.api.DataSourceFilter;

public class PerfJsonFilter extends DataSourceFilter<String> {

	private final int _maxCount;
	private int _count;
	
	public PerfJsonFilter(int maxCount){
		_maxCount = maxCount;
		_count = 0;
	}
	
	@Override
	protected JSONObject doFilter(String data) throws Exception {
		JSONObject obj = new JSONObject(data);
		long uid = _count % _maxCount;
		obj.put("uid",uid);
		_count++;
		return obj;
	}
}
