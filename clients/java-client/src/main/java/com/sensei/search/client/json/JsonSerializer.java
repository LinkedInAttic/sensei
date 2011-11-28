package com.sensei.search.client.json;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class JsonSerializer {
    public static Object serialize(Object object) throws JSONException {
        if (object == null) {
            return null;
        }
        if (object instanceof String || object instanceof Number || object instanceof Boolean || object.getClass().isPrimitive() || object instanceof JSONObject) {
            return object;
        }
        if (object.getClass().isEnum()) {
            return object.toString();
        }
        if (object instanceof Collection) {
            Collection collection = (Collection) object;
            List<Object> arr = new ArrayList<Object>(collection.size());
            for(Object obj : collection) {
                arr.add(serialize(obj));
            }
            return new JSONArray(arr);
        }
        if (object instanceof Map) {
            Map map = (Map) object;
            JSONObject ret = new JSONObject();
            for(Map.Entry entry : (Set<Map.Entry>) map.entrySet()) {
               ret.put(((String)entry.getKey()), serialize(entry.getValue()));
            }
            return ret;
        }
        // serialize object by reflection
        JSONObject ret = new JSONObject();
        for (Field field : object.getClass().getDeclaredFields()) {
            field.setAccessible(true);
            String name = field.getName();
            if (field.isAnnotationPresent(JsonField.class)) {
                name = field.getAnnotation(JsonField.class).value();
            }           
            try {
                ret.put(name, serialize(field.get(object)));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
       return ret;
    }
}
