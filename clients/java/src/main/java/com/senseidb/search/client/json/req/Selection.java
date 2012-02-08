package com.senseidb.search.client.json.req;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.json.JSONObject;

import com.senseidb.search.client.json.CustomJsonHandler;
import com.senseidb.search.client.json.req.filter.Filter;

@CustomJsonHandler(SelectionJsonHandler.class)
public abstract class Selection implements Filter {
  private String field;

  public String getField() {
    return field;
  }

  public Selection setField(String field) {
    this.field = field;
    return this;
  }

  public static class Custom extends Selection {
        private JSONObject custom;

        public Custom(JSONObject custom) {
            super();
            this.custom = custom;
        }
        public Custom() {
            // TODO Auto-generated constructor stub
        }
        public JSONObject getCustom() {
            return custom;
        }
    }
    public static Selection terms(String field, String... values) {
        if (values.length == 1) {
          return new Term(values[0]).setField(field);
        }
        return new Terms(Arrays.asList(values),new ArrayList<String>(), null).setField(field);
    }
    public static Selection terms(String field, List<String> values, List<String> excludes, Operator op) {
        return new Terms(values,excludes, op).setField(field);
    }
    public static Selection range(String field, String from, String to, boolean includeLower, boolean includeUpper) {
         return new Range(from, to, includeLower, includeUpper).setField(field);
    }
    public static Selection range(String field, String from , String to) {
        return new Range(from, to, true, true).setField(field);
   }
    public static Selection path(String field, String value, boolean strict, int depth) {
        return new Path(value, strict, depth).setField(field);
    }
    public static Selection custom(JSONObject custom) {

        return new Custom(custom);
    }
}
