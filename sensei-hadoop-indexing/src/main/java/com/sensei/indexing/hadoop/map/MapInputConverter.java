package com.sensei.indexing.hadoop.map;

import org.json.JSONException;
import org.json.JSONObject;

import com.sensei.indexing.api.JsonFilter;

public abstract class MapInputConverter extends JsonFilter   {


	/**
	 * @param key  key as mapper input;
	 * @param value   value as mapper input;
	 * @return  A JSONObject converted from the map input record;
	 * @throws JSONException
	 */
	public abstract JSONObject getJsonInput(Object key, Object value) throws JSONException;
	
	
	@Override
	protected abstract JSONObject doFilter(JSONObject data) throws Exception;
	
}
