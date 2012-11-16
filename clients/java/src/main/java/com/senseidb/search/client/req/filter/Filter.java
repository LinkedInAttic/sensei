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

package com.senseidb.search.client.req.filter;

import java.util.ArrayList;
import java.util.List;

import com.senseidb.search.client.req.Operator;

public interface Filter {

    /**
     * <p>
     * A filter that matches documents using <code>AND</code> or <code>OR</code>
     * boolean operator on other queries. This filter is more performant then <a
     * href="bool-filter.html">bool</a> filter. Can be placed within queries
     * that accept a filter.
     * </p>
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
