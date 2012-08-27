/**
 * This software is licensed to you under the Apache License, Version 2.0 (the
 * "Apache License").
 *
 * LinkedIn's contributions are made under the Apache License. If you contribute
 * to the Software, the contributions will be deemed to have been made under the
 * Apache License, unless you expressly indicate otherwise. Please do not make any
 * contributions that would be inconsistent with the Apache License.
 *
 * You may obtain a copy of the Apache License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, this software
 * distributed under the Apache License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the Apache
 * License for the specific language governing permissions and limitations for the
 * software governed under the Apache License.
 *
 * © 2012 LinkedIn Corp. All Rights Reserved.  
 */
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
          if (instance instanceof SenseiPlugin) {
            ((SenseiPlugin) instance).init(properties, senseiPluginRegistry);
            //((SenseiPlugin) instance).start();
          }
        } catch (Exception ex) {
          throw new RuntimeException(ex);
        }
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
    }
    return instance;
  }

}
