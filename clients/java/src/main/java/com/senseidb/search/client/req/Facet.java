/**
 * This software is licensed to you under the Apache License, Version 2.0 (the
 * "Apache License").
 *
 * LinkedIn's contributions are made under the Apache License. If you contribute
 * to the Software, the contributions will be deemed to have been made under the
 * Apache License, unless you expressly indicate otherwise. Please do not make any
 * contributions that would be inconsistent with the Apache License.
 *
 * You may obtain a copy of the Apache License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, this software
 * distributed under the Apache License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the Apache
 * License for the specific language governing permissions and limitations for the
 * software governed under the Apache License.
 *
 * © 2012 LinkedIn Corp. All Rights Reserved.  
 */

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
     * This parameter specifies how facet values should be ordered: <br>
     * • hits: order-by hits <br>
     * <br>
     * • val: order-by values
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

    public int getMax() {
        return max;
    }

    public int getMinHit() {
        return minHit;
    }

    public boolean isExpand() {
        return expand;
    }

    public OrderBy getOrder() {
        return order;
    }

    public Map<String, String> getProperties() {
        return properties;
    }
}
