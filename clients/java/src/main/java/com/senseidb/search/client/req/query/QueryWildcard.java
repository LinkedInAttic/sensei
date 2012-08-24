package com.senseidb.search.client.req.query;

import com.senseidb.search.client.json.CustomJsonHandler;

/**
 * <p>
 * A query that match on any (configurable) of the provided terms. This is a
 * simpler syntax query for using a <code>bool</code> query with several
 * <code>term</code> queries in the <code>should</code> clauses. For example:
 * </p>
 *
 *
 * <p>
 * The <code>terms</code> query is also aliased with <code>in</code> as the
 * query name for simpler usage.
 * </p>
 *
 *
 */
@CustomJsonHandler(QueryJsonHandler.class)
public class QueryWildcard extends FieldAwareQuery {
  private String value;
  private double boost;

  public QueryWildcard(String field, String value, double boost) {
    super();
    this.value = value;
    this.boost = boost;
    this.field = field;
  }
}
