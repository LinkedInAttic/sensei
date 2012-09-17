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

package com.senseidb.search.client.res;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.senseidb.search.client.json.JsonDeserializer;
import com.senseidb.search.client.json.JsonHandler;

public class SenseiHitJsonHandler implements JsonHandler<SenseiHit> {

  private static final Set<String> PREDEFINED_FIELDS= new HashSet<String>(Arrays.asList("uid", "docid", "score", "srcdata", "grouphitscount", "groupHits", "stored", "termvectors", "explanation"));

  @Override
  public JSONObject serialize(SenseiHit bean) throws JSONException {
    throw new UnsupportedOperationException();
    // return (JSONObject) JsonSerializer.serialize(bean);

  }

  @Override
  public SenseiHit deserialize(JSONObject json) throws JSONException {
    if (json == null) {
      return null;
    }

    /*SenseiHit senseiHit = new SenseiHit();
    senseiHit.setDocid(json.optInt("docid"));
    senseiHit.setGrouphitscount(json.optInt("grouphitscount"));
    senseiHit.setSrcdata(json.optString("srcdata"));
    senseiHit.setUid(json.optInt("uid"));
    senseiHit.setScore(json.optInt("score"));
    JSONArray groupHitsArr = json.optJSONArray("groupHits");
    if (groupHitsArr != null){
      List<SenseiHit> groupHits = new ArrayList<SenseiHit>(groupHitsArr.length());
      for (int i = 0; i< groupHitsArr.length(); i++) {
        groupHits.add(deserialize(groupHitsArr.optJSONObject(i)));
      }
      senseiHit.setGroupHits(groupHits);
    }*/
    SenseiHit senseiHit =  JsonDeserializer.deserialize(SenseiHit.class, json, false);
    JSONArray storedFieldsArr = json.optJSONArray("stored");
    if (storedFieldsArr != null) {
      List<FieldValue> storedFields = new ArrayList<FieldValue>(storedFieldsArr.length());
      for (int i = 0; i< storedFieldsArr.length(); i++) {
        JSONObject storedJson = storedFieldsArr.optJSONObject(i);
        if (storedJson != null) {
          String fieldName = (String) storedJson.keys().next();
          storedFields.add(new FieldValue(fieldName, storedJson.optString(fieldName)));
        }
      }
      senseiHit.setStoredFields(storedFields);
    }
    Iterator iterator = json.keys();
    while (iterator.hasNext()) {
      String field = (String) iterator.next();
      if (PREDEFINED_FIELDS.contains(field)) {
        continue;
      }
      JSONArray jsonArr = json.optJSONArray(field);
      if (jsonArr != null) {
        List<String> values = new ArrayList<String>(jsonArr.length());
        for (int i = 0; i< jsonArr.length(); i++) {
          values.add(jsonArr.getString(i));
        }
        senseiHit.getFieldValues().put(field, values);
      }
    }

    return senseiHit;
  }
}
