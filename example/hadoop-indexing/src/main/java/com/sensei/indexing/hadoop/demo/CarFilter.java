package com.sensei.indexing.hadoop.demo;

import org.json.JSONObject;

import com.sensei.indexing.api.JsonFilter;

public class CarFilter extends JsonFilter {

	@Override
	protected JSONObject doFilter(JSONObject data) throws Exception {
		return data;
	}

}
