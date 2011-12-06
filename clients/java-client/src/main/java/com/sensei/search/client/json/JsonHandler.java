package com.sensei.search.client.json;

import org.json.JSONException;
import org.json.JSONObject;

public interface JsonHandler<T> {
    JSONObject serialize(T bean) throws JSONException;
    T deserialize(JSONObject json) throws JSONException;
}
