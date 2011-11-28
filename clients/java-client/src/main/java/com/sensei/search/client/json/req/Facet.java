package com.sensei.search.client.json.req;


public class Facet {
    int max;int minCount; boolean expand; OrderBy orderBy;
    public static enum OrderBy {hits, val}
    public static class Builder {
        private Facet facet = new Facet();
        public Builder max(int max) {
           facet.max = max;
           return this;
        }
        public Builder minCount(int minCount) {
            facet.minCount = minCount;
            return this;
         }
        public Builder expand(boolean expand) {
            facet.expand = expand;
            return this;
        }
        public Builder orderByHits() {
            facet.orderBy = OrderBy.hits;
            return this;
        }
        public Builder orderByVal() {
            facet.orderBy = OrderBy.val;
            return this;
        }
        public Facet build() {
            return facet;
        }
    }
    public static Builder builder() {
        return new Builder();
    }
}
