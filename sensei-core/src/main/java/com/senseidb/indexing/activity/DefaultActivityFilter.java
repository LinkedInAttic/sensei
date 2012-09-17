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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.json.JSONException;
import org.json.JSONObject;

import com.senseidb.conf.SenseiSchema;
import com.senseidb.conf.SenseiSchema.FieldDefinition;
import com.senseidb.indexing.ShardingStrategy;
import com.senseidb.search.node.SenseiCore;

public class DefaultActivityFilter extends BaseActivityFilter {

  private volatile HashSet<String> cachedActivities;
@Override
public boolean acceptEventsForAllPartitions() {
  return false;
}
  @Override
  public ActivityFilteredResult filter(JSONObject event, SenseiSchema senseiSchema, ShardingStrategy shardingStrategy, SenseiCore senseiCore) {
    Map<Long, Map<String, Object>> columnValues= new HashMap<Long, Map<String, Object>>();
    Map<String, Object> innerMap = new  HashMap<String, Object>();
    long uid;
    try {
      uid = event.getLong(senseiSchema.getUidField());
    } catch (JSONException e) {
      throw new RuntimeException(e);
    }
    for (String activityField : getActivities(senseiSchema)) {
      Object obj = event.opt(activityField);
      if (obj != null) {
        event.remove(activityField);
        innerMap.put(activityField, obj);
      }
    }
    columnValues.put(uid, innerMap);
    ActivityFilteredResult activityFilteredResult = new ActivityFilteredResult(event, columnValues);
    return activityFilteredResult;
  }

  private Set<String> getActivities(SenseiSchema senseiSchema) {
    if (cachedActivities == null) {
      cachedActivities = new HashSet<String>();
      for (String  fieldName : senseiSchema.getFieldDefMap().keySet()) {
        FieldDefinition fieldDefinition = senseiSchema.getFieldDefMap().get(fieldName);
        if (fieldDefinition.isActivity) {
          cachedActivities.add(fieldName);
        }
      }
    }
    return cachedActivities;
  }

}
