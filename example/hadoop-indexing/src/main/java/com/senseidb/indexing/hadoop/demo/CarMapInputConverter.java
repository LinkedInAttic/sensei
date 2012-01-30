package com.senseidb.indexing.hadoop.demo;

import org.json.JSONException;
import org.json.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

import com.senseidb.indexing.hadoop.map.MapInputConverter;

public class CarMapInputConverter extends MapInputConverter {

	@Override
	public JSONObject getJsonInput(Object key, Object value, Configuration conf) throws JSONException {
		String line = ((Text) value).toString();
		return new JSONObject(line);
	}

	@Override
	protected JSONObject doFilter(JSONObject data) throws Exception {
		return data;
	}


}
