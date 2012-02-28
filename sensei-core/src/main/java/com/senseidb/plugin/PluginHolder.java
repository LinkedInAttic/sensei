package com.senseidb.plugin;

import java.util.LinkedHashMap;
import java.util.Map;

class PluginHolder {
  private final SenseiPluginRegistry senseiPluginRegistry;
  String pluginCLass;
  String pluginName;
  String fullPrefix;
  Object instance;
  private Object factoryCreatedInstance;
  Map<String, String> properties = new LinkedHashMap<String, String>();

  public PluginHolder(SenseiPluginRegistry senseiPluginRegistry,
                      String pluginCLass,
                      String pluginName,
                      String fullPrefix) {
    this.senseiPluginRegistry = senseiPluginRegistry;
    this.pluginCLass = pluginCLass;
    this.pluginName = pluginName;
    this.fullPrefix = fullPrefix;
  }

  public PluginHolder(SenseiPluginRegistry senseiPluginRegistry,
                      Object instance,
                      String pluginName,
                      String fullPrefix) {
    this.senseiPluginRegistry = senseiPluginRegistry;
    this.instance = instance;
    this.pluginName = pluginName;
    this.fullPrefix = fullPrefix;
  }

  public Object getInstance() {
    if (instance == null) {
      synchronized (this) {
        try {
          instance = Class.forName(pluginCLass).newInstance();
        } catch (Exception ex) {
          throw new RuntimeException(ex);
        }
      }
    }
    if (instance instanceof SenseiPlugin) {
      ((SenseiPlugin) instance).init(properties, senseiPluginRegistry);
      //((SenseiPlugin) instance).start();
    }
    if (instance instanceof SenseiPluginFactory) {
      if (factoryCreatedInstance == null) {
        synchronized (instance) {
          factoryCreatedInstance = ((SenseiPluginFactory) instance).getBean(properties, fullPrefix,
              this.senseiPluginRegistry);

        }
      }
      return factoryCreatedInstance;
    }
    return instance;
  }

}
