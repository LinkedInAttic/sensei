package com.senseidb.search.client.req.query;

import java.util.HashMap;
import java.util.Map;

import com.senseidb.search.client.json.CustomJsonHandler;
import com.senseidb.search.client.json.JsonField;

/**
 * User may supply his own implementation of the query
 *
 */
@CustomJsonHandler(value = QueryJsonHandler.class)
public class CustomQuery extends Query {
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
