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
