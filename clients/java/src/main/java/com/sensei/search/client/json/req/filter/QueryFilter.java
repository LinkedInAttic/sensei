package com.sensei.search.client.json.req.filter;

import com.sensei.search.client.json.CustomJsonHandler;
import com.sensei.search.client.json.req.query.Query;

@CustomJsonHandler(FilterJsonHandler.class)
public class QueryFilter implements Filter{
  private Query query;

  public QueryFilter(Query query) {
    super();
    this.query = query;
  }

  public Query getQuery() {
    return query;
  }

}
