package com.sensei.plugin;

import java.util.Map;

public interface SenseiPlugin {
	 public void init(Map<String, String> config);
	 public void start();
	 public void stop();
}
