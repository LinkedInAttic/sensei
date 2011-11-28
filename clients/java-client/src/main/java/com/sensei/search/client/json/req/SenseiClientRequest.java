package com.sensei.search.client.json.req;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.JSONException;
import org.json.JSONObject;


public class SenseiClientRequest {
    private ClientQuery query;
    private int from;
    private int size;
    private int count;
    private GroupBy groupBy;
    private Map<String, Selection> selections = new HashMap<String, Selection>();
    private Map<String, Map<String, FacetInit>> facetInit = new HashMap<String, Map<String, FacetInit>>();
    private List<Object> sorts = new ArrayList<Object>();
    private Map<String, Facet> facets = new HashMap<String, Facet>();
    private boolean fetchStored;
    private List<String> termVectors = new ArrayList<String>();
    private List<Integer> partitions = new ArrayList<Integer>();
    private boolean explain;
    private boolean routeParam;
    
    public static class Builder {
        private SenseiClientRequest request = new SenseiClientRequest();
        public Builder from(int from) {
            request.from = from;
            return this;
        }
        public Builder size(int size) {
            request.size = size;
            return this;
        }
        public Builder count(int count) {
            request.count = count;
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
        public Builder groupBy(String... columns) {
            request.groupBy = new GroupBy(Arrays.asList(columns));
            return this;
        }
        public Builder groupBy(List<String> columns) {
            request.groupBy = new GroupBy(columns);
            return this;
        }
        public Builder addSelection(String name, Selection selection) {
            request.selections.put(name, selection);
            return this;
        }
        public Builder addFacetInit(String name, Map<String, FacetInit> facetInits) {
            request.facetInit.put(name, facetInits);
            return this;
        }
        public Builder sortBy(String columnName) {
            request.sorts.add(columnName);
            return this;
        }
        public Builder sortByDesc(String columnName) {
            Map<String, String> oneEntryMap = new HashMap<String, String>();
            oneEntryMap.put(columnName, "desc");
            request.sorts.add(oneEntryMap);
            return this;
        }
        public Builder sortByRelevance() {
            request.sorts.add("_score");
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
	public void setQuery(ClientQuery query) {
		this.query = query;
	}
	public int getFrom() {
		return from;
	}
	public void setFrom(int from) {
		this.from = from;
	}
	public int getSize() {
		return size;
	}
	public void setSize(int size) {
		this.size = size;
	}
	public int getCount() {
		return count;
	}
	public void setCount(int count) {
		this.count = count;
	}
	public GroupBy getGroupBy() {
		return groupBy;
	}
	public void setGroupBy(GroupBy groupBy) {
		this.groupBy = groupBy;
	}
	public Map<String, Selection> getSelections() {
		return selections;
	}
	public void setSelections(Map<String, Selection> selections) {
		this.selections = selections;
	}
	public Map<String, Map<String, FacetInit>> getFacetInit() {
		return facetInit;
	}
	public void setFacetInit(Map<String, Map<String, FacetInit>> facetInit) {
		this.facetInit = facetInit;
	}
	public List<Object> getSorts() {
		return sorts;
	}
	public void setSorts(List<Object> sorts) {
		this.sorts = sorts;
	}
	public Map<String, Facet> getFacets() {
		return facets;
	}
	public void setFacets(Map<String, Facet> facets) {
		this.facets = facets;
	}
	public boolean isFetchStored() {
		return fetchStored;
	}
	public void setFetchStored(boolean fetchStored) {
		this.fetchStored = fetchStored;
	}
	public List<String> getTermVectors() {
		return termVectors;
	}
	public void setTermVectors(List<String> termVectors) {
		this.termVectors = termVectors;
	}
	public List<Integer> getPartitions() {
		return partitions;
	}
	public void setPartitions(List<Integer> partitions) {
		this.partitions = partitions;
	}
	public boolean isExplain() {
		return explain;
	}
	public void setExplain(boolean explain) {
		this.explain = explain;
	}
	public boolean isRouteParam() {
		return routeParam;
	}
	public void setRouteParam(boolean routeParam) {
		this.routeParam = routeParam;
	}
    
    
}
