package com.senseidb.search.node.impl;

import java.nio.charset.Charset;

import org.json.JSONException;
import org.json.JSONObject;

import com.senseidb.conf.SenseiSchema;
import com.senseidb.util.JSONUtil.FastJSONArray;
import com.senseidb.util.JSONUtil.FastJSONObject;

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
      return new FastJSONObject(new String(data,UTF8));
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
