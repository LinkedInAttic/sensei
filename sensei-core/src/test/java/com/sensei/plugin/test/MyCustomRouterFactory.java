package com.sensei.plugin.test;

import java.util.Map;

import com.sensei.plugin.SenseiPlugin;
import com.sensei.plugin.SenseiPluginRegistry;

public class MyCustomRouterFactory implements SenseiPlugin {

  public Map<String, String> config;
  public boolean started;

  @Override
  public void init(Map<String, String> config, SenseiPluginRegistry pluginRegistry) {
    this.config = config;
  }

  @Override
  public void start() {
   started = true;

  }

  @Override
  public void stop() {
    // TODO Auto-generated method stub

  }

}
