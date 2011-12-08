package com.sensei.plugin;

import java.util.Map;

public class AbstractSenseiPlugin implements SenseiPlugin {
  protected Map<String, ?> config;
  @Override
  public void init(Map<String, String> config) {
    this.config= config;
  }

  @Override
  public void start() {

  }

  @Override
  public void stop() {
  }

}
