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

import java.util.Map;

import org.json.JSONObject;

import com.senseidb.conf.SenseiSchema;
import com.senseidb.indexing.ShardingStrategy;
import com.senseidb.plugin.SenseiPlugin;
import com.senseidb.plugin.SenseiPluginRegistry;
import com.senseidb.search.node.SenseiCore;
import com.senseidb.util.Pair;

public abstract class BaseActivityFilter implements SenseiPlugin {
  protected Map<String, String> config;
  protected SenseiPluginRegistry pluginRegistry;
 
  @Override
  public final void init(Map<String, String> config, SenseiPluginRegistry pluginRegistry) {
    this.config = config;    
    this.pluginRegistry = pluginRegistry;    
  } 
  public  abstract ActivityFilteredResult filter(JSONObject event, SenseiSchema senseiSchema, ShardingStrategy shardingStrategy, SenseiCore senseiCore);
  
  public boolean acceptEventsForAllPartitions() {
    return false;
  }
  public static class ActivityFilteredResult {
    private JSONObject filteredObject;    
    private  Map<Long, Map<String, Object>> activityValues;
    public ActivityFilteredResult(JSONObject filteredObject, Map<Long, Map<String, Object>> activityValues) {
      super();
      this.filteredObject = filteredObject;
      this.activityValues = activityValues;
    }
    public JSONObject getFilteredObject() {
      return filteredObject;
    }
    public void setFilteredObject(JSONObject filteredObject) {
      this.filteredObject = filteredObject;
    }
    public Map<Long, Map<String, Object>> getActivityValues() {
      return activityValues;
    }
    public void setActivityValues(Map<Long, Map<String, Object>> activityValues) {
      this.activityValues = activityValues;
    }
    
  }
  @Override
  public void start() {
    
  }
  @Override
  public void stop() {
    
  }
}
