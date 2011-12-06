package com.sensei.search.client.json;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class JsonSerializer {
    public static Object serialize(Object object) throws JSONException {
        return serialize(object,true);
    }
    public static Object serialize(Object object, boolean handleCustomJsonHandler) throws JSONException {
        if (object == null) {
            return null;
        }
        if (object instanceof String || object instanceof Number || object instanceof Boolean || object.getClass().isPrimitive() || object instanceof JSONObject) {
            return object;
        }
        CustomJsonHandler customJsonHandler = getCustomJsonHandlerByType(object.getClass());
        if (customJsonHandler != null && handleCustomJsonHandler) {
          JsonHandler jsonHandler = instantiate(customJsonHandler.value());
            return jsonHandler.serialize(object);
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
                CustomJsonHandler customJsonHandlerAnnotation = getCustomJsonHandlerByField(field);
                Object fieldValue = field.get(object);
                if (customJsonHandlerAnnotation == null) {
                    ret.put(name, serialize(fieldValue));
                } else {
                    JsonHandler jsonHandler = instantiate(customJsonHandlerAnnotation.value());
                    Object fieldJson =  jsonHandler.serialize(fieldValue);
                    if (customJsonHandlerAnnotation.flatten() && fieldJson != null) {
                        String[] names = JSONObject.getNames((JSONObject)fieldJson);
                        if (names == null || names.length != 1) {
                            throw new IllegalStateException("It's impossible to flatten the JsonExpression " + fieldJson);
                        }
                        Object internalJson = ((JSONObject) fieldJson).opt(names[0]);
                        if (customJsonHandlerAnnotation.overrideColumnName()) {
                            name = names[0];
                        }
                        fieldJson = internalJson;
                    }
                    ret.put(name, fieldJson);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
       return ret;
    }
    private static Map<Class<?>, CustomJsonHandler> jsonHandlersByType = Collections.synchronizedMap(new WeakHashMap<Class<?>, CustomJsonHandler>());
    private static Map<Field, CustomJsonHandler> jsonHandlersByField = Collections.synchronizedMap(new WeakHashMap<Field, CustomJsonHandler>());
    private static Map<Class<? extends JsonHandler>, JsonHandler> jsonHandlers = Collections.synchronizedMap(new WeakHashMap<Class<? extends JsonHandler>, JsonHandler>());
    private static CustomJsonHandler getCustomJsonHandlerByType(Class<?> cls) {
        if (!jsonHandlersByType.containsKey(cls)) {
            CustomJsonHandler customJsonHandler = (CustomJsonHandler) ReflectionUtil.getAnnotation(cls, CustomJsonHandler.class);
            jsonHandlersByType.put(cls, customJsonHandler);
        }
        return jsonHandlersByType.get(cls);
    }
    private static CustomJsonHandler getCustomJsonHandlerByField(Field field) {
        if (!jsonHandlersByField.containsKey(field)) {
            CustomJsonHandler customJsonHandler = field.getAnnotation(CustomJsonHandler.class);
            jsonHandlersByField.put(field, customJsonHandler);
        }
        return jsonHandlersByField.get(field);
    }
    private static JsonHandler instantiate(Class<? extends JsonHandler> cls) {
        if (!jsonHandlers.containsKey(cls)) {

            try {
                jsonHandlers.put(cls, cls.newInstance());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return jsonHandlers.get(cls);
    }
}
