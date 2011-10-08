package com.sensei.indexing.hadoop.map;

import org.json.JSONObject;

import com.sensei.indexing.api.JsonFilter;

public class DummyFilter extends JsonFilter {

	@Override
	protected JSONObject doFilter(JSONObject data) throws Exception {
		return data;
	}

}
