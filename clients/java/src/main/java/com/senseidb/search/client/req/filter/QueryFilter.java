package com.senseidb.search.client.req.filter;

import com.senseidb.search.client.json.CustomJsonHandler;
import com.senseidb.search.client.req.query.Query;

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
