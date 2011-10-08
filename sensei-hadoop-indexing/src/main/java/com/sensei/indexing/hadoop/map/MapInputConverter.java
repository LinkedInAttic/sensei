package com.sensei.indexing.hadoop.map;

import org.json.JSONException;
import org.json.JSONObject;

public interface MapInputConverter {


	/**
	 * @param key  key as mapper input;
	 * @param value   value as mapper input;
	 * @return  A JSONObject converted from the map input record;
	 * @throws JSONException
	 */
	public JSONObject getJsonInput(Object key, Object value) throws JSONException;
}
