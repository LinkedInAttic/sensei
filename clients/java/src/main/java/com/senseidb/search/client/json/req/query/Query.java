package com.senseidb.search.client.json.req.query;

import com.senseidb.search.client.json.req.filter.Filter;
import com.senseidb.search.client.json.req.relevance.Relevance;

public abstract class Query implements Filter {
    private Relevance  relevance;
    
    public Query setRelevance(Relevance  relevance) {
      this.relevance = relevance;
      return this;
    }

    public Relevance getRelevance() {
      return relevance;
    }
    
}
