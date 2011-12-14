package com.sensei.plugin;

import java.util.Map;

public interface SenseiPlugin {
	 public void init(Map<String, String> config, SenseiPluginRegistry pluginRegistry);
	 public void start();
	 public void stop();
}
