package com.sensei.search.client.json.req;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Assert;


public class SenseiClientRequest {
    private ClientQuery query;
    private Paging paging;
   
    
    private GroupBy groupBy;
    private List<SelectionContainer> selections = new ArrayList<SelectionContainer>();
    private Map<String, Map<String, FacetInit>> facetInit = new HashMap<String, Map<String, FacetInit>>();
    private List<Sort> sorts = new ArrayList<Sort>();
    private Map<String, Facet> facets = new HashMap<String, Facet>();
    private boolean fetchStored;
    private List<String> termVectors = new ArrayList<String>();
    private List<Integer> partitions = new ArrayList<Integer>();
    private boolean explain;
    private boolean routeParam;
    
    public static class Builder {
        private SenseiClientRequest request = new SenseiClientRequest();
        public Builder paging(int count, int offset) {
            request.paging = new Paging(count, offset);
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
        
        public Builder query(String query) {
            try {
                request.query = new ClientQuery(new JSONObject(query));
            } catch (JSONException e) {
                throw new RuntimeException(e);
            }
            return this;
        }
        public Builder query(JSONObject query) {
            request.query = new ClientQuery(query);
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
        public Builder addSelection(SelectionContainer selection) {
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
        public SenseiClientRequest build() {
            return request;
        }
    }
    public static Builder builder() {
        return new Builder();
    }
    public ClientQuery getQuery() {
        return query;
    }
    public Paging getPaging() {
        return paging;
    }
    public GroupBy getGroupBy() {
        return groupBy;
    }
    public List<SelectionContainer> getSelections() {
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
    public boolean isRouteParam() {
        return routeParam;
    }

    
    
}
