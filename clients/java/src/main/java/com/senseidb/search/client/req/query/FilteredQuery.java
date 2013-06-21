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

package com.senseidb.search.client.req.query;

import com.senseidb.search.client.json.CustomJsonHandler;
import com.senseidb.search.client.req.filter.Filter;
import com.senseidb.search.client.req.filter.FilterJsonHandler;

/**
 * <p>
 * A query that applies a filter to the results of another query. This query
 * maps to Sensei <code>FilteredQuery</code>.
 * </p>
 * 
 * 
 * <p>
 * The filter object can hold only filter elements, not queries. Filters can be
 * much faster compared to queries since they don’t perform any scoring,
 * especially when they are cached.
 * </p>
 * 
 * 
 */
@CustomJsonHandler(value = QueryJsonHandler.class)
public class FilteredQuery extends Query {

    private Query query;
    @CustomJsonHandler(value = FilterJsonHandler.class, flatten = false)
    private Filter filter;
    private double boost;

    public FilteredQuery(Query query, Filter filter, double boost) {
        super();
        this.query = query;
        this.filter = filter;
    }

}
