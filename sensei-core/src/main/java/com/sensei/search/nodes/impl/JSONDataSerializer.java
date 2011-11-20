package com.sensei.search.nodes.impl;

import org.json.JSONObject;

import com.sensei.conf.SenseiSchema;

import proj.zoie.store.ZoieStoreSerializer;

public class JSONDataSerializer implements ZoieStoreSerializer<JSONObject>{

  private final String _uidField;
  public JSONDataSerializer(SenseiSchema schema){
    _uidField = schema.getUidField();
    
  }
  
  @Override
  public long getUid(JSONObject data) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public byte[] toBytes(JSONObject data) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public JSONObject fromBytes(byte[] data) {
    // TODO Auto-generated method stub
    return null;
  }

}
