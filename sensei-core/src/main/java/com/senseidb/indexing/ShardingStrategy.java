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
package com.senseidb.indexing;

import java.util.Map;

import org.json.JSONObject;
import org.json.JSONException;

import com.senseidb.conf.SenseiSchema;

import com.senseidb.plugin.SenseiPluginFactory;
import com.senseidb.plugin.SenseiPluginRegistry;

public interface ShardingStrategy {
  int caculateShard(int maxShardId,JSONObject dataObj) throws JSONException;

  public static class FieldModShardingStrategy implements ShardingStrategy
  {
    public static class Factory implements SenseiPluginFactory<FieldModShardingStrategy>
    {
      @Override
      public FieldModShardingStrategy getBean(Map<String, String> initProperties,
                                              String fullPrefix,
                                              SenseiPluginRegistry pluginRegistry)
      {
        return new FieldModShardingStrategy(initProperties.get("field"));
      }
    }

    protected String _field;

    public FieldModShardingStrategy(String field)
    {
      _field = field;
    }

    @Override
    public int caculateShard(int maxShardId,JSONObject dataObj) throws JSONException
    {
      JSONObject event = dataObj.optJSONObject(SenseiSchema.EVENT_FIELD);
      long uid;
      if (event == null)
        uid = Long.parseLong(dataObj.getString(_field));
      else
        uid = Long.parseLong(event.getString(_field));
      return (int)(uid % maxShardId);
    }
  }
}
