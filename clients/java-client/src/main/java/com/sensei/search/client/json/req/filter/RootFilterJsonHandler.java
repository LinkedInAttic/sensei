package com.sensei.search.client.json.req.filter;

import java.lang.reflect.Field;

import org.json.JSONException;
import org.json.JSONObject;

import com.sensei.search.client.json.JsonHandler;

public class RootFilterJsonHandler implements JsonHandler<FilterRoot>{
    private FilterJsonHandler filterJsonHandler = new FilterJsonHandler();
    @Override
    public JSONObject serialize(FilterRoot bean) throws JSONException {
        if (bean == null) {
            return null;
        }
        JSONObject ret = new JSONObject(); 
        for(Field field : FilterRoot.class.getDeclaredFields()) {
            field.setAccessible(true);
            try {
                JSONObject jsonFilter = filterJsonHandler.serialize((Filter) field.get(bean));
                if (jsonFilter == null) {
                    continue;
                }
                String name = JSONObject.getNames(jsonFilter)[0];
                ret.put(name, jsonFilter.opt(name));
            } catch (Exception e) {
               throw new RuntimeException(e);
            }
        }
        return ret;
    }

    @Override
    public FilterRoot deserialize(JSONObject json) throws JSONException {
        
        return null;
    }
    
}
