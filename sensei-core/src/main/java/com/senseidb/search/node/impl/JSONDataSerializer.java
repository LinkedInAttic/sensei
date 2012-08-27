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
package com.senseidb.search.node.impl;

import java.nio.charset.Charset;

import org.json.JSONException;
import org.json.JSONObject;

import com.senseidb.conf.SenseiSchema;

import proj.zoie.store.ZoieStoreSerializer;

public class JSONDataSerializer implements ZoieStoreSerializer<JSONObject>{

  private static Charset UTF8 = Charset.forName("UTF-8");
  private final String _uidField;
  private final String _delField;
  private final String _skipFieldField;
  
  
  public JSONDataSerializer(SenseiSchema schema){
    _uidField = schema.getUidField();
    _delField = schema.getDeleteField();
    _skipFieldField = schema.getSkipField();
  }
  
  @Override
  public long getUid(JSONObject data) {
    try {
      return Long.parseLong(data.optString(_uidField, "-1"));
    }
    catch(Exception e) {
      return -1L;
    }
  }

  @Override
  public byte[] toBytes(JSONObject data) {
    return data.toString().getBytes(UTF8);
  }

  @Override
  public JSONObject fromBytes(byte[] data) {
    try {
      return new JSONObject(new String(data,UTF8));
    } catch (JSONException e) {
      return null;
    }
  }

  @Override
  public boolean isDelete(JSONObject obj) {
	  return obj.optBoolean(_delField);
  }

  @Override
  public boolean isSkip(JSONObject obj) {
	  return obj.optBoolean(_skipFieldField);
  }
}
