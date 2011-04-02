package com.sensei.indexing.api;

import org.json.JSONObject;

public interface JsonFilter {
  JSONObject filter(JSONObject obj) throws Exception;
}
