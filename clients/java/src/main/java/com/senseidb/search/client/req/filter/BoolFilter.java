package com.senseidb.search.client.req.filter;

import java.util.List;

import com.senseidb.search.client.json.CustomJsonHandler;
import com.senseidb.search.client.json.JsonField;

/**
 * <p>
 * A filter that matches documents matching boolean combinations of other
 * queries. Similar in concept to Boolean query, except that the clauses are
 * other filters. Can be placed within queries that accept a filter.
 * </p>
 * 
 * 
 */
@CustomJsonHandler(FilterJsonHandler.class)
public class BoolFilter implements Filter {
  List<Filter> must;
  List<Filter> must_not;
  List<Filter> should;
  @JsonField("minimum_number_should_match")
  Boolean minimumNumberShouldMatch;
  Double boost;
  Boolean disableCoord;

  public BoolFilter(List<Filter> must, List<Filter> must_not, List<Filter> should) {
    super();
    this.must = must;
    this.must_not = must_not;
    this.should = should;
  }

  public List<Filter> getMust() {
    return must;
  }

  public void setMust(List<Filter> must) {
    this.must = must;
  }

  public List<Filter> getMust_not() {
    return must_not;
  }

  public void setMust_not(List<Filter> must_not) {
    this.must_not = must_not;
  }

  public List<Filter> getShould() {
    return should;
  }

  public void setShould(List<Filter> should) {
    this.should = should;
  }

}