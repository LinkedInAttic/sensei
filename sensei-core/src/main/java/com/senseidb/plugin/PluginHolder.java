package com.senseidb.plugin;

import java.util.LinkedHashMap;
import java.util.Map;

class PluginHolder {
    private final SenseiPluginRegistry senseiPluginRegistry;
    String pluginClass;
    String pluginName;
    String fullPrefix;
    Object instance;
    private Object factoryCreatedInstance;
    Map<String, String> properties = new LinkedHashMap<String, String>();

    public PluginHolder(SenseiPluginRegistry senseiPluginRegistry,
            String pluginClass,
            String pluginName,
            String fullPrefix) {
        this.senseiPluginRegistry = senseiPluginRegistry;
        this.pluginClass = pluginClass;
        this.pluginName = pluginName;
        this.fullPrefix = fullPrefix;
    }

    public synchronized Object getInstance() {
        if (instance == null) {
            try {
                instance = Class.forName(pluginClass).newInstance();
                if (instance instanceof SenseiPlugin) {
                    ((SenseiPlugin) instance).init(properties, senseiPluginRegistry);
                    //((SenseiPlugin) instance).start();
                }
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
        if (instance instanceof SenseiPluginFactory) {
            if (factoryCreatedInstance == null) {
                synchronized (instance) {
                    factoryCreatedInstance =
                        ((SenseiPluginFactory) instance).getBean(properties,
                            fullPrefix,
                            this.senseiPluginRegistry);
                }
            }
            return factoryCreatedInstance;
        } else {
            return instance;
        }
    }

}
