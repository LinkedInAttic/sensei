package com.senseidb.search.client.req.query.span;

import com.senseidb.search.client.json.CustomJsonHandler;
import com.senseidb.search.client.req.query.Query;
import com.senseidb.search.client.req.query.QueryJsonHandler;

/**
 * <p>
 * Removes matches which overlap with another span query. The span not query
 * maps to Sensei <code>SpanNotQuery</code>. Here is an example:
 * </p>
 *
 *
 * <p>
 * The <code>include</code> and <code>exclude</code> clauses can be any span
 * type query. The <code>include</code> clause is the span query whose matches
 * are filtered, and the <code>exclude</code> clause is the span query whose
 * matches must not overlap those returned.
 * </p>
 *
 *
 */
@CustomJsonHandler(QueryJsonHandler.class)
public class SpanNot extends Query {

  SpanTerm include;

  SpanTerm exclude;

  private final double boost;

  public SpanNot(SpanTerm include, SpanTerm exclude, double boost) {
    super();
    this.include = include;
    include.setBoost(null);
    exclude.setBoost(null);
    this.exclude = exclude;
    this.boost = boost;
  }

}
