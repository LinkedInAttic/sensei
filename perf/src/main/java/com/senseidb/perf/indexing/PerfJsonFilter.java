package com.senseidb.perf.indexing;

import org.json.JSONObject;

import com.sensei.indexing.api.JsonFilter;

public class PerfJsonFilter extends JsonFilter {

	private final int _maxCount;
	private int _count;
	
	public PerfJsonFilter(int maxCount){
		_maxCount = maxCount;
		_count = 0;
	}
	
	@Override
	protected JSONObject doFilter(JSONObject data) throws Exception {
		long uid = _count % _maxCount;
		data.put("perf-id",uid);
		_count++;
		return data;
	}
}
