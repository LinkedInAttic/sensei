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
import com.senseidb.search.client.req.Term;

/**
 * <p>
 * A query that generates the union of documents produced by its subqueries, and
 * that scores each document with the maximum score for that document as
 * produced by any subquery, plus a tie breaking increment for any additional
 * matching subqueries.
 * </p>
 * <p>
 * This is useful when searching for a word in multiple fields with different
 * boost factors (so that the fields cannot be combined equivalently into a
 * single search field). We want the primary score to be the one associated with
 * the highest boost, not the sum of the field scores (as Boolean Query would
 * give). If the query is “albino elephant” this ensures that “albino” matching
 * one field and “elephant” matching another gets a higher score than “albino”
 * matching both fields. To get this result, use both Boolean Query and
 * DisjunctionMax Query: for each term a DisjunctionMaxQuery searches for it in
 * each field, while the set of these DisjunctionMaxQuery’s is combined into a
 * BooleanQuery.
 * </p>
 * <p>
 * The tie breaker capability allows results that include the same term in
 * multiple fields to be judged better than results that include this term in
 * only the best of those multiple fields, without confusing this with the
 * better case of two different terms in the multiple fields.The default
 * <code>tie_breaker</code> is <code>0.0</code>.
 * </p>
 * <p>
 * This query maps to Sensei <code>DisjunctionMaxQuery</code>.
 * </p>
 *
 */
@CustomJsonHandler(QueryJsonHandler.class)
public class DisMax extends Query {
  @JsonField("tie_braker")
  private double tieBraker;
  private double boost;
  private List<Term> queries;

  public DisMax(double tieBraker, List<Term> queries, double boost) {
    super();
    this.tieBraker = tieBraker;
    this.boost = boost;
    this.queries = queries;
  }

}
