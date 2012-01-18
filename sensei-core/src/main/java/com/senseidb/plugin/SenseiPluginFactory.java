package com.senseidb.plugin;

import java.util.Map;

public interface SenseiPluginFactory<T> {
    T getBean(Map<String,String> initProperties, String fullPrefix, SenseiPluginRegistry pluginRegistry);
}
