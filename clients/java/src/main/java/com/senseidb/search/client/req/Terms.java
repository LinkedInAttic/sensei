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

import java.util.List;

import com.senseidb.search.client.json.JsonField;
import com.senseidb.search.client.req.query.Query;

/**
 * <p>
 * A query that match on any (configurable) of the provided terms. This is a
 * simpler syntax query for using a <code>bool</code> query with several
 * <code>term</code> queries in the <code>should</code> clauses. For example:
 * </p>
 *
 *
 * <p>
 * The <code>terms</code> query is also aliased with <code>in</code> as the
 * query name for simpler usage.
 * </p>
 *
 *
 */
public class Terms extends Selection {

  List<String> values;
  List<String> excludes;
  Operator operator;
  Double boost;
  @JsonField("minimum_match")
  Integer minimumMatch;
  boolean _noOptimize = false;

  public Terms() {

  }

  public Terms(List<String> values, List<String> excludes, Operator op) {
    super();
    this.values = values;
    this.excludes = excludes;
    this.operator = op;
  }

  public Terms(List<String> values, List<String> excludes, Operator op, int minimumMatch, double boost) {
    super();
    this.values = values;
    this.excludes = excludes;
    this.operator = op;
    this.boost = boost;
    this.minimumMatch = minimumMatch;
  }

  public List<String> getValues() {
    return values;
  }

  public List<String> getExcludes() {
    return excludes;
  }

  public Operator getOperator() {
    return operator;
  }
  

}
