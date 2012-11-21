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

import java.util.List;

import com.senseidb.search.client.json.CustomJsonHandler;
import com.senseidb.search.client.json.JsonField;

/**
 * <p>
 * A query that matches documents matching boolean combinations of other
 * queries. Similar in concept to Boolean query, except that the clauses are
 * other filters. Can be placed within queries that accept a filter.
 * </p>
 * 
 * 
 * 
 */

@CustomJsonHandler(QueryJsonHandler.class)
public class BoolQuery extends Query {
    private List<Query> must;
    private List<Query> must_not;
    private List<Query> should;
    @JsonField("minimum_number_should_match")
    private Integer minimumNumberShouldMatch;
    private double boost;
    private Boolean disableCoord;

    public BoolQuery(List<Query> must, List<Query> must_not,
            List<Query> should, Integer minimumNumberShouldMatch, double boost,
            Boolean disableCoord) {
        super();
        this.must = must;
        this.must_not = must_not;
        this.should = should;
        this.minimumNumberShouldMatch = minimumNumberShouldMatch;
        this.boost = boost;
        this.disableCoord = disableCoord;
    }

    public List<Query> getMust() {
        return must;
    }

    public List<Query> getMust_not() {
        return must_not;
    }

    public List<Query> getShould() {
        return should;
    }

    public Integer getMinimumNumberShouldMatch() {
        return minimumNumberShouldMatch;
    }

    public double getBoost() {
        return boost;
    }

    public Boolean getDisableCoord() {
        return disableCoord;
    }
    
    @Override 
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("(");
        if (must != null) {
            for (Query q : must) {
                builder.append("+").append("(").append(q).append(")");
            }
        }
        if (must_not != null) {
            for (Query q : must_not) {
                builder.append("-").append("(").append(q).append(")");
            }
        }
        if (should != null) {
            for (Query q : should) {
                builder.append("(").append(q).append(")");
            }
        }
        builder.append("~").append(minimumNumberShouldMatch);
        builder.append("^").append(boost);
        builder.append(")");
        return builder.toString();
    }

}
