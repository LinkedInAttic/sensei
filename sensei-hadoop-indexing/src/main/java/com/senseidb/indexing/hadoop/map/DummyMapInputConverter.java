package com.senseidb.indexing.hadoop.map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.json.JSONException;
import org.json.JSONObject;

import com.senseidb.util.JSONUtil.FastJSONArray;
import com.senseidb.util.JSONUtil.FastJSONObject;

public class DummyMapInputConverter extends MapInputConverter {

	@Override
	public JSONObject getJsonInput(Object key, Object value, Configuration conf) throws JSONException {
		String line = ((Text) value).toString();
		return new FastJSONObject(line);
	}

	@Override
	protected JSONObject doFilter(JSONObject data) throws Exception {
		return data;
	}

}
