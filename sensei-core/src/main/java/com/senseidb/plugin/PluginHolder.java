package com.senseidb.plugin;

import java.util.LinkedHashMap;
import java.util.Map;

class PluginHolder {
    private final SenseiPluginRegistry senseiPluginRegistry;
    private final String pluginClass;
    private final String fullPrefix;
    private Object instance;
    private Object factoryCreatedInstance;
    private final Map<String, String> properties;

    public PluginHolder(SenseiPluginRegistry senseiPluginRegistry,
            String pluginClass,
            String fullPrefix,
            Map<String, String> properties) {
        this.senseiPluginRegistry = senseiPluginRegistry;
        this.pluginClass = pluginClass;
        this.fullPrefix = fullPrefix;
        this.properties = properties;
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
