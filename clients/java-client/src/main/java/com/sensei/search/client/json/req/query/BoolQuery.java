package com.sensei.search.client.json.req.query;

import java.util.List;

import com.sensei.search.client.json.CustomJsonHandler;
import com.sensei.search.client.json.JsonField;

/**
 * <p>
 * A query that matches documents matching boolean combinations of other
 * queries. Similar in concept to Boolean query, except that the clauses are
 * other filters. Can be placed within queries that accept a filter.
 * </p>
 *
 *
 *
 */

@CustomJsonHandler(QueryJsonHandler.class)
public class BoolQuery implements Query {
  List<Query> must;
  List<Query> must_not;
  List<Query> should;
  @JsonField("minimum_number_should_match")
  Integer minimumNumberShouldMatch;
  double boost;
  Boolean disableCoord;

  public BoolQuery(List<Query> must, List<Query> must_not, List<Query> should, Integer minimumNumberShouldMatch,
      double boost, Boolean disableCoord) {
    super();
    this.must = must;
    this.must_not = must_not;
    this.should = should;
    this.minimumNumberShouldMatch = minimumNumberShouldMatch;
    this.boost = boost;
    this.disableCoord = disableCoord;
  }

}
