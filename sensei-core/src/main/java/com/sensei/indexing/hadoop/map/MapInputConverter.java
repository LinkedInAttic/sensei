package com.sensei.indexing.hadoop.map;

import org.json.JSONException;
import org.json.JSONObject;

public interface MapInputConverter {

    public int getUID(JSONObject json) throws JSONException;

	public JSONObject getJsonInput(Object key, Object value) throws JSONException;
}
