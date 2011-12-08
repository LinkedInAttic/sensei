package com.sensei.plugin;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.configuration.Configuration;

import com.browseengine.bobo.facets.FacetHandler;

public class SenseiPluginRegistry {
  public static final String FACET_CONF_PREFIX = "sensei.custom.facets";
  private Map<String, SenseiPluginRegistry.PluginHolder> pluginsByPrefix = new LinkedHashMap<String, SenseiPluginRegistry.PluginHolder>();
  private Map<String, PluginHolder> pluginsByNames = new LinkedHashMap<String, SenseiPluginRegistry.PluginHolder>();
  private List<PluginHolder> plugins = new ArrayList<PluginHolder>();
  private Configuration configuration;
  private SenseiPluginRegistry() {

  }
  private class PluginHolder {
    String pluginCLass;
    String pluginName;
    String fullPrefix;
    Object instance;
    private Object factoryCreatedInstance;
    Map<String, String> properties = new LinkedHashMap<String, String>();

    public PluginHolder(String pluginCLass, String pluginName, String fullPrefix) {
      super();
      this.pluginCLass = pluginCLass;
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
        ((SenseiPlugin) instance).init(properties);
        ((SenseiPlugin) instance).start();
      }
      if (instance instanceof SenseiPluginFactory) {
        if (factoryCreatedInstance == null) {
          synchronized (instance) {
            factoryCreatedInstance = ((SenseiPluginFactory) instance).getBean(properties, fullPrefix,
                SenseiPluginRegistry.this);

          }
        }
        return factoryCreatedInstance;
      }
      return instance;
    }

  }

  public static synchronized SenseiPluginRegistry build(Configuration conf) {
    SenseiPluginRegistry ret = new SenseiPluginRegistry();
    ret.configuration = conf;
    Iterator keysIterator = conf.getKeys();
    while (keysIterator.hasNext()) {
      String key = (String) keysIterator.next();
      if (key.endsWith(".class")) {
        String prefix = key.substring(0, key.indexOf(".class"));
        String pluginName = prefix;
        if (prefix.contains(".")) {
          pluginName = prefix.substring(prefix.lastIndexOf(".") + 1);
        }
        String pluginCLass = conf.getString(key);
        ret.plugins.add(ret.new PluginHolder(pluginCLass, pluginName, prefix));
      }
    }
    for (PluginHolder pluginHolder : ret.plugins) {
      ret.pluginsByPrefix.put(pluginHolder.fullPrefix, pluginHolder);
      ret.pluginsByNames.put(pluginHolder.pluginName, pluginHolder);

      Iterator propertyIterator = conf.getKeys(pluginHolder.fullPrefix);
      while (propertyIterator.hasNext()) {
        String propertyName = (String) propertyIterator.next();
        if (propertyName.endsWith(".class")) {
          continue;
        }
        String property = propertyName;
        if (propertyName.contains(".")) {
          property = propertyName.substring(propertyName.lastIndexOf(".") + 1);
        }
        pluginHolder.properties.put(property, conf.getString(propertyName));

      }
    }
    return ret;
  }

  public <T> T getBeanByName(String name, Class<T> type) {
    PluginHolder holder = pluginsByNames.get(name);
    if (holder == null) {
      return null;
    }
    return (T) holder.getInstance();
  }

  public <T> List<T> getBeansByType(Class<T> type) {
    List<T> ret = new ArrayList<T>();
    for (PluginHolder pluginHolder : plugins) {
      if (pluginHolder.getInstance() != null && type.isAssignableFrom(pluginHolder.getInstance().getClass())) {
        ret.add((T) pluginHolder.getInstance());
      }
    }
    return ret;
  }

  public List<?> getBeansByPrefix(String prefix) {
    List<Object> ret = new ArrayList<Object>();
    for (PluginHolder pluginHolder : plugins) {
      if (pluginHolder.getInstance() != null && pluginHolder.fullPrefix.startsWith(prefix)) {
        ret.add(pluginHolder.getInstance());
      }
    }
    return ret;
  }
  public FacetHandler<?> getFacet(String name) {
    for (FacetHandler handler : resolveBeansByListKey(FACET_CONF_PREFIX, FacetHandler.class)) {
      if (handler.getName().equals(name)) {
        return handler;
      }
    }
    return null;
  }
  public <T> T getBeanByFullPrefix(String fullPrefix, Class<T> type) {
    PluginHolder holder = pluginsByPrefix.get(fullPrefix);
    if (holder == null) {
      return null;
    }
    return (T) holder.getInstance();
  }

  public <T> List<T> resolveBeansByListKey(String paramKey, Class<T> returnedClass) {
    if (!paramKey.endsWith(".list")) {
      paramKey = paramKey + ".list";
    }

    List<T> ret = new ArrayList<T>();
    String strList = configuration.getString(paramKey);
    if (strList == null) {
      return null;
    }
    String[] keys = strList.split(",");
    if (keys == null || keys.length == 0) {
      return null;
    }
    for (String key : keys) {
      if (key.trim().length() == 0) {
        continue;
      }
      Object bean = getBeanByName(key.trim(), Object.class);
      if (bean == null) {
        bean = getBeanByFullPrefix(key.trim(), Object.class);
      }
      if (bean == null) {
        throw new IllegalStateException("the bean with name " + key + " couldn't be found");
      }
      if (bean instanceof Collection) {
        ret.addAll((Collection) bean);
      } else {
        ret.add((T) bean);
      }
    }
    return ret;
  }

  public synchronized void start() {

  }

  public synchronized void stop() {
    for (PluginHolder pluginHolder : plugins) {
      if (pluginHolder.instance instanceof SenseiPlugin) {
        ((SenseiPlugin) pluginHolder.instance).stop();
      }
    }
  }
}
