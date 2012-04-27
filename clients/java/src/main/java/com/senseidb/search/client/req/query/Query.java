package com.senseidb.search.client.req.query;

import com.senseidb.search.client.req.filter.Filter;
import com.senseidb.search.client.req.relevance.Relevance;

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
