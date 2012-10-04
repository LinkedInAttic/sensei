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
package com.senseidb.indexing.activity;

import org.apache.commons.configuration.Configuration;

import com.senseidb.plugin.SenseiPluginRegistry;

public class ActivityConfig  {
  private int flushBufferSize = 50000;
  private int flushBufferMaxDelayInSeconds = 15;
  private int purgeJobFrequencyInSeconds = 0;
  private int undeletableBufferSize = 500;
  public  ActivityConfig(SenseiPluginRegistry pluginRegistry) {
    flushBufferSize = getInt(pluginRegistry.getConfiguration(), "flushBufferSize", 50000);
    flushBufferMaxDelayInSeconds = getInt(pluginRegistry.getConfiguration(), "flushBufferMaxDelayInSeconds", 15);
    purgeJobFrequencyInSeconds = getInt(pluginRegistry.getConfiguration(), "purgeJobFrequencyInMinutes", 0);
    undeletableBufferSize = getInt(pluginRegistry.getConfiguration(), "undeletableBufferSize", 500);
    
  }
  public ActivityConfig() {
 }
  private static int getInt(Configuration configuration, String key, int defaultValue) {
    String compoundKey = "sensei.activity.config." + key;
    return configuration.getInt(compoundKey, defaultValue);
  }
 
  public int getFlushBufferSize() {
    return flushBufferSize;
  }
  public int getFlushBufferMaxDelayInSeconds() {
    return flushBufferMaxDelayInSeconds;
  }
  public int getPurgeJobFrequencyInMinutes() {
    return purgeJobFrequencyInSeconds;
  }
  public int getUndeletableBufferSize() {
    return undeletableBufferSize;
  }
  
}
