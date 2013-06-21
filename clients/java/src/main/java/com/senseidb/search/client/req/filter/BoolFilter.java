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

import java.util.List;

import com.senseidb.search.client.json.CustomJsonHandler;
import com.senseidb.search.client.json.JsonField;

/**
 * <p>
 * A filter that matches documents matching boolean combinations of other
 * queries. Similar in concept to Boolean query, except that the clauses are
 * other filters. Can be placed within queries that accept a filter.
 * </p>
 * 
 * 
 */
@CustomJsonHandler(FilterJsonHandler.class)
public class BoolFilter implements Filter {
    List<Filter> must;
    List<Filter> must_not;
    List<Filter> should;
    @JsonField("minimum_number_should_match")
    Boolean minimumNumberShouldMatch;
    Double boost;
    Boolean disableCoord;

    public BoolFilter(List<Filter> must, List<Filter> must_not,
            List<Filter> should) {
        super();
        this.must = must;
        this.must_not = must_not;
        this.should = should;
    }

    public List<Filter> getMust() {
        return must;
    }

    public void setMust(List<Filter> must) {
        this.must = must;
    }

    public List<Filter> getMust_not() {
        return must_not;
    }

    public void setMust_not(List<Filter> must_not) {
        this.must_not = must_not;
    }

    public List<Filter> getShould() {
        return should;
    }

    public void setShould(List<Filter> should) {
        this.should = should;
    }

    public Boolean getMinimumNumberShouldMatch() {
        return minimumNumberShouldMatch;
    }

    public Double getBoost() {
        return boost;
    }

    public Boolean getDisableCoord() {
        return disableCoord;
    }

}
