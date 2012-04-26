package com.senseidb.search.client.req.query;

public class FieldAwareQuery extends Query {
  protected String field;

  public FieldAwareQuery setField(String field) {
    this.field = field;
    return this;
  }

  public String getField() {
    return field;
  }

}
