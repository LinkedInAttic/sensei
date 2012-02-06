package com.senseidb.search.client.json.req.filter;

import java.util.ArrayList;
import java.util.List;

import com.senseidb.search.client.json.req.Operator;

public interface Filter {

  /**
   * <p>
   * A filter that matches documents using <code>AND</code> or <code>OR</code>
   * boolean operator on other queries. This filter is more performant then <a
   * href="bool-filter.html">bool</a> filter. Can be placed within queries that
   * accept a filter.
   * </p>
   * 
   * <pre class="prettyprint lang-js">
   * </pre>
   * 
   * 
   * 
   */
  public static class AndOr implements Filter {
    List<Filter> filters = new ArrayList<Filter>();;
    Operator operation;

    public AndOr(List<Filter> filters, Operator operation) {
      super();
      this.filters = filters;
      this.operation = operation;
    }

    public List<Filter> getFilters() {
      return filters;
    }

    public Operator getOperation() {
      return operation;
    }

  }
}
