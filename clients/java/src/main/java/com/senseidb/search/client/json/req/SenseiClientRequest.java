package com.senseidb.search.client.json.req;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.senseidb.search.client.json.CustomJsonHandler;
import com.senseidb.search.client.json.req.filter.Filter;
import com.senseidb.search.client.json.req.filter.FilterJsonHandler;
import com.senseidb.search.client.json.req.query.Query;
import com.senseidb.search.client.json.req.query.QueryJsonHandler;

/**
 * The sensei request object, that is used to send the Sensei query to the
 * server
 *
 */
public class SenseiClientRequest {
  /* *//**
   * @see com.senseidb.search.client.json.req.Paging
   */
  /*
   * private Paging paging;
   */
  private Integer size;
  private Integer from;

  /**
   *
   * @see com.senseidb.search.client.json.req.GroupBy
   *
   */
  private GroupBy groupBy;
  private List<Selection> selections = new ArrayList<Selection>();
  @CustomJsonHandler(value = QueryJsonHandler.class)
  private Query query;
  /**
   * Initializing parameters for runtime facet handlers: a map that contains the
   * initializing parameters that are needed by all runtime facet handlers
   */
  private Map<String, Map<String, FacetInit>> facetInit = new HashMap<String, Map<String, FacetInit>>();
  private List<Sort> sorts = new ArrayList<Sort>();
  private Map<String, Facet> facets = new HashMap<String, Facet>();
  /**
   * Flag indicating whether stored fields are to be fetched
   */
  private boolean fetchStored;
  private List<String> termVectors = new ArrayList<String>();
  /**
   * shards of the index to be searched
   */
  private List<Integer> partitions = new ArrayList<Integer>();
  /**
   * Flag indicating whether explanation information should be returned
   */
  private boolean explain;
  /**
   * the field value used for routing
   */
  private String routeParam;
  @CustomJsonHandler(value = FilterJsonHandler.class)
  private Filter filter;

  /**
   * Allows template substitution on the server. The template occurrence in
   * other places should begin with the dollar sign<br>
   * Example: <br>
   * { { "query_string" : { "query" : "$color1 or $color2", boost :
   * "$customBoost" }, { templateMapping {color1:"red", color2:"blue",
   * customBoost : 1.0}} } <br>
   * will produce<br>
   * { { "query_string" : { "query" : "red or blue, boost : 1.0 }
   *
   * } on the server
   */
  private Map<String, Object> templateMapping;

  /**
   * @author vzhabiuk
   *
   */

  public static class Builder {
    private SenseiClientRequest request = new SenseiClientRequest();

    public Builder paging(int size, int offset) {
      request.size = size;
      request.from = offset;
      return this;
    }

    public Builder fetchStored(boolean fetchStored) {
      request.fetchStored = fetchStored;
      return this;
    }

    public Builder partitions(List<Integer> partitions) {
      request.partitions = partitions;
      return this;
    }

    public Builder explain(boolean explain) {
      request.explain = explain;
      return this;
    }

    public Builder query(Query query) {
      request.query = query;

      return this;
    }

    public Builder groupBy(int top, String... columns) {
      request.groupBy = new GroupBy(Arrays.asList(columns), top);
      return this;
    }

    public Builder groupBy(List<String> columns, int top) {
      request.groupBy = new GroupBy(columns, top);
      return this;
    }

    public Builder addSelection(Selection selection) {
      if (selection == null) {
        throw new IllegalArgumentException("The selectionContainer should be not null");
      }
      request.selections.add(selection);
      return this;
    }

    public Builder addFacetInit(String name, Map<String, FacetInit> facetInits) {
      request.facetInit.put(name, facetInits);
      return this;
    }

    /**
     * @see com.senseidb.search.client.json.req.SenseiClientRequest#templateMapping
     */

    public Builder addTemplateMapping(String name, Object value) {
      if (request.templateMapping == null) {
        request.templateMapping = new HashMap<String, Object>();
      }
      request.templateMapping.put(name, value);
      return this;
    }

    public Builder addSort(Sort sort) {
      if (sort == null) {
        throw new IllegalArgumentException("The sort should be not null");
      }
      request.sorts.add(sort);
      return this;
    }

    public Builder addTermVector(String term) {
      request.termVectors.add(term);
      return this;
    }

    public Builder addFacetInit(String name, String parameter, FacetInit facetInit) {
      if (!request.facetInit.containsKey(name)) {
        request.facetInit.put(name, new HashMap<String, FacetInit>());
      }
      request.facetInit.get(name).put(parameter, facetInit);
      return this;
    }

    public Builder addFacet(String name, Facet facet) {
      request.facets.put(name, facet);
      return this;
    }

    public Builder routeParam(String routeParam) {
      request.routeParam = routeParam;
      return this;
    }

    public Builder filter(Filter filter) {
      request.filter = filter;
      return this;
    }

    public SenseiClientRequest build() {
      return request;
    }
  }

  public static Builder builder() {
    return new Builder();
  }

  public Paging getPaging() {
    return new Paging(size, from);
  }

  public GroupBy getGroupBy() {
    return groupBy;
  }

  public List<Selection> getSelections() {
    return selections;
  }

  public Map<String, Map<String, FacetInit>> getFacetInit() {
    return facetInit;
  }

  public List<Sort> getSorts() {
    return sorts;
  }

  public Map<String, Facet> getFacets() {
    return facets;
  }

  public boolean isFetchStored() {
    return fetchStored;
  }

  public List<String> getTermVectors() {
    return termVectors;
  }

  public List<Integer> getPartitions() {
    return partitions;
  }

  public boolean isExplain() {
    return explain;
  }

  public String getRouteParam() {
    return routeParam;
  }

  public Integer getCount() {
    return size;
  }

  public Integer getFrom() {
    return from;
  }

  public Query getQuery() {
    return query;
  }

  public Filter getFilter() {
    return filter;
  }

  public Map<String, Object> getTemplateMapping() {
    return templateMapping;
  }

  public void setQuery(Query query) {
    this.query = query;
  }

  public void setFilter(Filter filter) {
    this.filter = filter;
  }

  public void setSelections(List<Selection> selections) {
    this.selections = selections;
  }
  
}
