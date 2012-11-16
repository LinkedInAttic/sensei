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

import com.senseidb.search.client.json.JsonField;
import com.senseidb.search.client.req.query.Query;

/**
 * <p>
 * Matches documents with fields that have terms within a certain range. The
 * type of the Sensei query depends on the field type, for <code>string</code>
 * fields, the <code>TermRangeQuery</code>, while for number/date fields, the
 * query is a <code>NumericRangeQuery</code>. The following example returns all
 * documents where <code>age</code> is between <code>10</code> and
 * <code>20</code>:
 * </p>
 * 
 * 
 * <p>
 * The <code>range</code> query top level parameters include:
 * </p>
 * <table>
 * <tbody>
 * <tr>
 * <th>Name</th>
 * <th>Description</th>
 * </tr>
 * <tr>
 * 
 * <td> <code>from</code></td>
 * <td>The lower bound. Defaults to start from the first.</td>
 * </tr>
 * <tr>
 * <td> <code>to</code></td>
 * 
 * <td>The upper bound. Defaults to unbounded.</td>
 * </tr>
 * <tr>
 * <td> <code>include_lower</code></td>
 * <td>Should the first from (if set) be inclusive or not. Defaults to
 * <code>true</code></td>
 * 
 * </tr>
 * <tr>
 * <td> <code>include_upper</code></td>
 * <td>Should the last to (if set) be inclusive or not. Defaults to
 * <code>true</code>.</td>
 * </tr>
 * 
 * 
 * 
 * 
 * 
 */
public class Range extends Selection {

    private String from;
    private String to;
    @JsonField("include_lower")
    private boolean includeLower;
    @JsonField("include_upper")
    private boolean includeUpper;

    private Double boost;
    @JsonField("_noOptimize")
    private Boolean notOptimize;
    private String type;

    public Range() {
    }

    public Range(String from, String to, boolean includeLower,
            boolean includeUpper) {
        super();

        this.from = from;
        this.to = to;
        this.includeLower = includeLower;
        this.includeUpper = includeUpper;
    }

    public Range(String from, String to, boolean includeLower,
            boolean includeUpper, double boost, boolean noOptimize) {
        super();

        this.from = from;
        this.to = to;
        this.includeLower = includeLower;
        this.includeUpper = includeUpper;
        this.boost = boost;
        notOptimize = noOptimize;
    }

    public Range(String from, String to, boolean includeLower,
            boolean includeUpper, Double boost, boolean noOptimize, String type) {
        super();

        this.from = from;
        this.to = to;
        this.includeLower = includeLower;
        this.includeUpper = includeUpper;
        this.boost = boost;
        notOptimize = noOptimize;
        this.type = type;
    }

    public String getFrom() {
        return from;
    }

    public String getTo() {
        return to;
    }

    public boolean isIncludeLower() {
        return includeLower;
    }

    public boolean isIncludeUpper() {
        return includeUpper;
    }

    public Double getBoost() {
        return boost;
    }

    public Boolean getNotOptimize() {
        return notOptimize;
    }

    public String getType() {
        return type;
    }

}
