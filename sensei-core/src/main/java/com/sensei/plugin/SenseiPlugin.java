package com.sensei.plugin;

import org.json.JSONObject;

public abstract class SenseiPlugin {
	abstract public void initialize(JSONObject config) throws Exception;
}
