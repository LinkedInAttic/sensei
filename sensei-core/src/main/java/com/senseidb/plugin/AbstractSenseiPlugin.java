package com.senseidb.plugin;

import java.util.Map;

public class AbstractSenseiPlugin implements SenseiPlugin {
  protected Map<String, String> config;
  protected SenseiPluginRegistry pluginRegistry;
  @Override
  public void init(Map<String, String> config, SenseiPluginRegistry pluginRegistry) {
    this.config= config;
    this.pluginRegistry = pluginRegistry;
  }

  @Override
  public void start() {

  }

  @Override
  public void stop() {
  }

}
