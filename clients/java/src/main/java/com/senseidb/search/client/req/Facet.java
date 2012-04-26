package com.senseidb.search.client.req;

import java.util.HashMap;
import java.util.Map;

public class Facet {
  /**
   * This parameter specifies the maximum count value for a facet
   */
  int max;
  /**
     *
     */
  /**
   * Minimum hits parameter
   */
  int minHit;
  /**
   * Selection-expand parameter
   */
  boolean expand;
  /**
   * This parameter specifies how facet values should be ordered:
   * <br>• hits: order-by hits <br>
   * <br>• val: order-by values
   */
  OrderBy order;
  
  Map<String, String> properties = new HashMap<String, String>();
  
  public static enum OrderBy {
    hits, val
  }

  public static class Builder {
    private Facet facet = new Facet();

    public Builder max(int max) {
      facet.max = max;
      return this;
    }

    public Builder minHit(int minCount) {
      facet.minHit = minCount;
      return this;
    }

    public Builder expand(boolean expand) {
      facet.expand = expand;
      return this;
    }

    public Builder orderByHits() {
      facet.order = OrderBy.hits;
      return this;
    }

    public Builder orderByVal() {
      facet.order = OrderBy.val;
      return this;
    }
    public Builder addProperty(String name, String value) {
      facet.properties.put(name, value);
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
