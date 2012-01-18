package com.senseidb.search.client.json.req;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.json.JSONObject;

import com.senseidb.search.client.json.CustomJsonHandler;
import com.senseidb.search.client.json.JsonField;
import com.senseidb.search.client.json.req.filter.Filter;
import com.senseidb.search.client.json.req.query.Query;

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

  public static class Path extends Selection {

    private String value;
    private boolean strict;
    private int depth;

    public Path(String value, boolean strict, int depth) {
      super();

      this.value = value;
      this.strict = strict;
      this.depth = depth;
    }

    public Path() {

    }
  }

  /**
   * <p>
   * Matches documents with fields that have terms within a certain range. The
   * type of the Sensei query depends on the field type, for <code>string</code>
   * fields, the <code>TermRangeQuery</code>, while for number/date fields, the
   * query is a <code>NumericRangeQuery</code>. The following example returns
   * all documents where <code>age</code> is between <code>10</code> and
   * <code>20</code>:
   * </p>
   *
  *
   * <p>
   * The <code>range</code> query top level parameters include:
   * </p>
   * <table>
   * <tbody>
   * <tr>
   * <th>Name</th>
   * <th>Description</th>
   * </tr>
   * <tr>
   *
   * <td> <code>from</code></td>
   * <td>The lower bound. Defaults to start from the first.</td>
   * </tr>
   * <tr>
   * <td> <code>to</code></td>
   *
   * <td>The upper bound. Defaults to unbounded.</td>
   * </tr>
   * <tr>
   * <td> <code>include_lower</code></td>
   * <td>Should the first from (if set) be inclusive or not. Defaults to
   * <code>true</code></td>
   *
   * </tr>
   * <tr>
   * <td> <code>include_upper</code></td>
   * <td>Should the last to (if set) be inclusive or not. Defaults to
   * <code>true</code>.</td>
   * </tr>
   *
   *
   *
   *
   *
   */
    public static class Range extends Selection implements Query{

      private String from;
        private String to;
        @JsonField("include_lower")
        private boolean includeLower;
        @JsonField("include_upper")
        private boolean includeUpper;

        private Double boost;
        @JsonField("_noOptimize")
        private Boolean notOptimize;
        private String type;
       public Range() {
       }
    public Range(String from, String to, boolean includeLower, boolean includeUpper) {
        super();

        this.from = from;
        this.to = to;
        this.includeLower = includeLower;
        this.includeUpper = includeUpper;
    }
    public Range(String from, String to, boolean includeLower, boolean includeUpper, double Doost, boolean noOptimize) {
      super();

      this.from = from;
      this.to = to;
      this.includeLower = includeLower;
      this.includeUpper = includeUpper;
      this.boost = boost;
      notOptimize = noOptimize;
  }
    public Range(String from, String to, boolean includeLower, boolean includeUpper, Double boost, boolean noOptimize, String type) {
      super();

      this.from = from;
      this.to = to;
      this.includeLower = includeLower;
      this.includeUpper = includeUpper;
      this.boost = boost;
      notOptimize = noOptimize;
      this.type = type;
  }
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
