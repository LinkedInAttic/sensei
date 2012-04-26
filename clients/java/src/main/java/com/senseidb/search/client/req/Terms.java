package com.senseidb.search.client.req;

import java.util.List;

import com.senseidb.search.client.json.JsonField;
import com.senseidb.search.client.req.query.Query;

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
public class Terms extends Selection {

  List<String> values;
  List<String> excludes;
  Operator operator;
  Double boost;
  @JsonField("minimum_match")
  Integer minimumMatch;
  boolean _noOptimize = false;

  public Terms() {

  }

  public Terms(List<String> values, List<String> excludes, Operator op) {
    super();
    this.values = values;
    this.excludes = excludes;
    this.operator = op;
  }

  public Terms(List<String> values, List<String> excludes, Operator op, int minimumMatch, double boost) {
    super();
    this.values = values;
    this.excludes = excludes;
    this.operator = op;
    this.boost = boost;
    this.minimumMatch = minimumMatch;
  }

  public List<String> getValues() {
    return values;
  }

  public List<String> getExcludes() {
    return excludes;
  }

  public Operator getOperator() {
    return operator;
  }
  

}