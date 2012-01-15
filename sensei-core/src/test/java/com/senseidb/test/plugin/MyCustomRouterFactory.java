package com.senseidb.test.plugin;

import java.util.Map;

import com.senseidb.plugin.SenseiPlugin;
import com.senseidb.plugin.SenseiPluginRegistry;

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
