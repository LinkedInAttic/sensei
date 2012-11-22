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

import java.util.Arrays;
import java.util.List;

import com.senseidb.search.client.json.CustomJsonHandler;
import com.senseidb.search.client.json.JsonField;
import com.senseidb.search.client.req.Operator;
import com.senseidb.search.client.req.filter.Filter;

/**
 * <p>
 * A query that uses a query parser in order to parse its content. Here is an
 * example:
 * </p>
 * 
 * <table>
 * <tbody>
 * <tr>
 * <th>Parameter</th>
 * <th>Description</th>
 * </tr>
 * <tr>
 * 
 * <td> <code>query</code></td>
 * <td>The actual query to be parsed.</td>
 * </tr>
 * <tr>
 * <td> <code>default_field</code></td>
 * 
 * <td>The default field for query terms if no prefix field is specified.
 * Defaults to the <code>_all</code> field.</td>
 * </tr>
 * <tr>
 * <td> <code>default_operator</code></td>
 * <td>The default operator used if no explicit operator is specified. For
 * example, with a default operator of <code>OR</code>, the query
 * <code>capital of Hungary</code> is translated to
 * <code>capital OR of OR Hungary</code>, and with default operator of
 * <code>AND</code>, the same query is translated to
 * <code>capital AND of AND Hungary</code>. The default value is <code>OR</code>
 * .</td>
 * 
 * </tr>
 * <tr>
 * <td> <code>analyzer</code></td>
 * <td>The analyzer name used to analyze the query string.</td>
 * </tr>
 * <tr>
 * 
 * <td> <code>allow_leading_wildcard</code></td>
 * <td>When set, <code>*</code> or <code>?</code> are allowed as the first
 * character. Defaults to <code>true</code>.</td>
 * 
 * </tr>
 * <tr>
 * <td> <code>lowercase_expanded_terms</code></td>
 * <td>Whether terms of wildcard, prefix, fuzzy, and range queries are to be
 * automatically lower-cased or not (since they are not analyzed). Default it
 * <code>true</code>.</td>
 * </tr>
 * 
 * <tr>
 * <td> <code>enable_position_increments</code></td>
 * <td>Set to <code>true</code> to enable position increments in result queries.
 * Defaults to <code>true</code>.</td>
 * </tr>
 * 
 * <tr>
 * <td> <code>fuzzy_prefix_length</code></td>
 * <td>Set the prefix length for fuzzy queries. Default is <code>0</code>.</td>
 * </tr>
 * <tr>
 * 
 * <td> <code>fuzzy_min_sim</code></td>
 * <td>Set the minimum similarity for fuzzy queries. Defaults to
 * <code>0.5</code></td>
 * </tr>
 * <tr>
 * <td> <code>phrase_slop</code></td>
 * 
 * <td>Sets the default slop for phrases. If zero, then exact phrase matches are
 * required. Default value is <code>0</code>.</td>
 * </tr>
 * <tr>
 * <td> <code>boost</code></td>
 * <td>Sets the boost value of the query. Defaults to <code>1.0</code>.</td>
 * 
 * </tr>
 * <tr>
 * <td> <code>analyze_wildcard</code></td>
 * <td>By default, wildcards terms in a query string are not analyzed. By
 * setting this value to <code>true</code>, a best effort will be made to
 * analyze those as well.</td>
 * </tr>
 * 
 * <tr>
 * <td> <code>auto_generate_phrase_queries</code></td>
 * <td>Default to <code>false</code>.</td>
 * </tr>
 * <tr>
 * 
 * <td> <code>minimum_should_match</code></td>
 * <td>A percent value (for example <code>20%</code>) controlling how many
 * “should” clauses in the resulting boolean query should match.</td>
 * </tr>
 * </tbody>
 * </table>
 * <p>
 * When a multi term query is being generated, one can control how it gets
 * rewritten using the <a href="multi-term-rewrite.html">rewrite</a> parameter.
 * </p>
 * 
 * <h1>Multi Field</h1>
 * <p>
 * The <code>query_string</code> query can also run against multiple fields. The
 * idea of running the <code>query_string</code> query against multiple fields
 * is by internally creating several queries for the same query string, each
 * with <code>default_field</code> that match the fields provided. Since several
 * queries are generated, combining them can be automatically done either using
 * a <code>dis_max</code> query or a simple <code>bool</code> query. For example
 * (the <code>name</code> is boosted by 5 using <code>^5</code> notation):
 * </p>
 * 
 * <p>
 * When running the <code>query_string</code> query against multiple fields, the
 * following additional parameters are allowed:
 * </p>
 * <table>
 * <tbody>
 * <tr>
 * <th>Parameter</th>
 * <th>Description</th>
 * </tr>
 * <tr>
 * 
 * <td> <code>use_dis_max</code></td>
 * <td>Should the queries be combined using <code>dis_max</code> (set it to
 * <code>true</code>), or a <code>bool</code> query (set it to
 * <code>false</code>). Defaults to <code>true</code>.</td>
 * 
 * </tr>
 * <tr>
 * <td> <code>tie_breaker</code></td>
 * <td>When using <code>dis_max</code>, the disjunction max tie breaker.
 * Defaults to <code>0</code>.</td>
 * 
 * </tr>
 * </tbody>
 * </table>
 * <p>
 * The fields parameter can also include pattern based field names, allowing to
 * automatically expand to the relevant fields (dynamically introduced fields
 * included). For example:
 * </p>
 * 
 */
@CustomJsonHandler(QueryJsonHandler.class)
public class StringQuery extends Query {
    @JsonField("default_field")
    private String defaultField;
    private String query;
    @JsonField("default_operator")
    private Operator defaultOperator;
    @JsonField("allow_leading_wildCard")
    private Boolean allowLeadingWildCard;
    @JsonField("lowercase_expanded_terms")
    private Boolean lowercaseExpandedTerms;

    @JsonField("enable_position_increments")
    private Boolean enablePositionIncrements;
    @JsonField("fuzzy_prefix_length")
    private String fuzzyPrefixLength;
    @JsonField("fuzzy_min_sim")
    private Double fuzzyMinSim;
    @JsonField("phrase_slop")
    private Integer phraseSlop;
    private Double boost = 1.0;
    @JsonField("auto_generate_phrase_queries")
    private Boolean autoGeneratePhraseQueries;
    private List<String> fields;
    @JsonField("use_dis_max")
    private Boolean useDisMax;
    @JsonField("tie_breaker")
    private Integer tieBreaker;

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private StringQuery query = new StringQuery();

        public Builder defaultField(String defaultField) {
            query.defaultField = defaultField;
            return this;
        }

        public Builder allowLeadingWildCard(boolean allowLeadingWildCard) {
            query.allowLeadingWildCard = allowLeadingWildCard;
            return this;
        }

        public Builder defaultOperator(Operator op) {
            query.defaultOperator = op;
            return this;
        }

        public Builder lowercaseExpandedTerms(boolean lowercaseExpandedTerms) {
            query.lowercaseExpandedTerms = lowercaseExpandedTerms;
            return this;
        }

        public Builder enablePositionIncrements(boolean enablePositionIncrements) {
            query.enablePositionIncrements = enablePositionIncrements;
            return this;
        }

        public Builder fuzzyPrefixLength(String fuzzyPrefixLength) {
            query.fuzzyPrefixLength = fuzzyPrefixLength;
            return this;
        }

        public Builder fuzzyMinSim(double fuzzyMinSim) {
            query.fuzzyMinSim = fuzzyMinSim;
            return this;
        }

        public Builder phraseSlop(int phraseSlop) {
            query.phraseSlop = phraseSlop;
            return this;
        }

        public Builder boost(double boost) {
            query.boost = boost;
            return this;
        }

        public Builder autoGeneratePhraseQueries(
                boolean autoGeneratePhraseQueries) {
            query.autoGeneratePhraseQueries = autoGeneratePhraseQueries;
            return this;
        }

        public Builder fields(String... fields) {
            query.fields = Arrays.asList(fields);
            return this;
        }

        public Builder useDisMax(boolean useDisMax) {
            query.useDisMax = useDisMax;
            return this;
        }

        public Builder tieBreaker(int tieBreaker) {
            query.tieBreaker = tieBreaker;
            return this;
        }

        public Builder query(String queryParam) {
            this.query.query = queryParam;
            return this;
        }

        public StringQuery build() {
            return query;
        }
    }

    public String getDefaultField() {
        return defaultField;
    }

    public String getQuery() {
        return query;
    }

    public Operator getDefaultOperator() {
        return defaultOperator;
    }

    public Boolean getAllowLeadingWildCard() {
        return allowLeadingWildCard;
    }

    public Boolean getLowercaseExpandedTerms() {
        return lowercaseExpandedTerms;
    }

    public Boolean getEnablePositionIncrements() {
        return enablePositionIncrements;
    }

    public String getFuzzyPrefixLength() {
        return fuzzyPrefixLength;
    }

    public Double getFuzzyMinSim() {
        return fuzzyMinSim;
    }

    public Integer getPhraseSlop() {
        return phraseSlop;
    }

    public Double getBoost() {
        return boost;
    }

    public Boolean getAutoGeneratePhraseQueries() {
        return autoGeneratePhraseQueries;
    }

    public List<String> getFields() {
        return fields;
    }

    public Boolean getUseDisMax() {
        return useDisMax;
    }

    public Integer getTieBreaker() {
        return tieBreaker;
    }

    @Override
    public String toString() {
        return query;
    }

}
