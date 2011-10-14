package com.sensei.indexing.hadoop.map;

import org.apache.hadoop.io.Text;
import org.json.JSONException;
import org.json.JSONObject;

public class DummyMapInputConverter extends MapInputConverter {

	@Override
	public JSONObject getJsonInput(Object key, Object value) throws JSONException {
		String line = ((Text) value).toString();
		return new JSONObject(line);
	}

	@Override
	protected JSONObject doFilter(JSONObject data) throws Exception {
		return data;
	}

}
