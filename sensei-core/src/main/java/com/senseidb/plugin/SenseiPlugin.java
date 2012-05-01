package com.senseidb.plugin;

import java.util.Map;

/**
 * Base interface for sensei plugins.
 * The lifecicle of a SenseiPlugin instance will include calls to init, start and stop,
 * exactly in that order, and exactly once during the life of the object.
 * It's expected that the plugin becomes functional only after calling start.
 */
public interface SenseiPlugin {
	 public void init(Map<String, String> config, SenseiPluginRegistry pluginRegistry);
	 public void start();
	 public void stop();
}
