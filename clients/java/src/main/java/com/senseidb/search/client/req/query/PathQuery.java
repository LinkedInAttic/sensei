package com.senseidb.search.client.req.query;

import com.senseidb.search.client.json.CustomJsonHandler;

@CustomJsonHandler(value = QueryJsonHandler.class)
public class PathQuery extends FieldAwareQuery {
  private String value;
  private double boost;

  public PathQuery(String field, String value, double boost) {
    super();
    this.value = value;
    this.boost = boost;
    this.field = field;
  }

}
