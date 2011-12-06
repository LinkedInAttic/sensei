package com.sensei.search.client.json.req.query;

import java.util.HashMap;
import java.util.Map;

import com.sensei.search.client.json.CustomJsonHandler;
import com.sensei.search.client.json.JsonField;

/**
 * User may supplu his own implementation of the query
 *
 */
@CustomJsonHandler(value = QueryJsonHandler.class)
public class CustomQuery implements Query {
  @JsonField("class")
  private String cls;
  private Map<String, String> params = new HashMap<String, String>();
  private double boost;

  public CustomQuery(String cls, Map<String, String> params, double boost) {
    super();
    this.cls = cls;
    this.params = params;
    this.boost = boost;
  }

}
