package com.senseidb.search.client.json.req;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.json.JSONException;
import org.json.JSONObject;

import com.senseidb.search.client.json.JsonDeserializer;
import com.senseidb.search.client.json.JsonHandler;
import com.senseidb.search.client.json.JsonSerializer;

public class SelectionJsonHandler implements JsonHandler<Selection> {
    public static Map<String, Class<? extends Selection>> selectionClasses = new HashMap<String, Class<? extends Selection>>();
    static {
        for (Class<? extends Selection> cls : Arrays.asList(Term.class, Terms.class, Path.class, Range.class, Selection.Custom.class)) {
            selectionClasses.put(cls.getSimpleName().toLowerCase(), cls);
        }
    }
    @Override
    public JSONObject serialize(Selection bean)  throws JSONException {
        if (bean == null) {
            return null;
        }
        if (bean instanceof Selection.Custom) {
            JSONObject ret = new JSONObject();
            ret.put("custom", ((Selection.Custom)bean).getCustom());
            return ret;
        }
        
        JSONObject innerObject = (JSONObject) JsonSerializer.serialize(bean, false);
        JSONObject paramContainer = new JSONObject();
        paramContainer.put(bean.getField(), innerObject);
        JSONObject ret = new JSONObject();
        ret.put(bean.getClass().getSimpleName().toLowerCase(), paramContainer);
        return ret;
    }

    @Override
    public Selection deserialize(JSONObject json)  throws JSONException{
        String[] names = JSONObject.getNames(json);
        if (names.length == 0 || !selectionClasses.keySet().contains(names[0])) {
            throw new IllegalStateException("The json object doesn't contain the value from " + Arrays.toString(names));
        }
        String name = names[0];
        JSONObject innerPart = json.getJSONObject(name);
        if ("custom".equals(name)) {
            return Selection.custom(innerPart);
        }
        String fieldName = JSONObject.getNames(innerPart)[0];
        Selection selection = JsonDeserializer.deserialize(selectionClasses.get(name), innerPart.getJSONObject(fieldName), false);
        selection.setField(fieldName);
        return selection;
    }

}
