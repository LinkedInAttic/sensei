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

import java.util.Arrays;
import java.util.List;

import com.senseidb.search.client.req.Operator;
import com.senseidb.search.client.req.Path;
import com.senseidb.search.client.req.Range;
import com.senseidb.search.client.req.Selection;
import com.senseidb.search.client.req.Term;
import com.senseidb.search.client.req.Terms;
import com.senseidb.search.client.req.filter.Filter.AndOr;
import com.senseidb.search.client.req.query.Query;

public class Filters {
    public static Ids ids(List<String> values, List<String> excludes) {
        return new Ids(values, excludes);
    }
    public static AndOr and(Filter... filters) {
        return new AndOr(Arrays.asList(filters), Operator.and);
    }
    public static AndOr or(Filter... filters) {
        return new AndOr(Arrays.asList(filters), Operator.or);
    }

    public static QueryFilter query(Query query) {
        return new QueryFilter(query);

    }

    public static BoolFilter bool(List<Filter> must, List<Filter> must_not, List<Filter> should) {
        return new BoolFilter(must, must_not, should);
    }
    public static BoolFilter boolMust(Filter... must) {
        return new BoolFilter(Arrays.asList(must), null, null);
    }
    public static BoolFilter boolMustNot(Filter... mustNot) {
        return new BoolFilter( null, Arrays.asList(mustNot), null);
    }
    public static BoolFilter boolShould(Filter... should) {
        return new BoolFilter( null, null, Arrays.asList(should));
    }
    public static IsNull isNull(String fieldName) {
      return new IsNull(fieldName);
  }
    public static Term term(String field, String value) {
        return (Term)new Term(value).setField(field);
    }
    public static Selection terms(String field, List<String> values, List<String> excludes, Operator op) {
        return new Terms(values,excludes, op).setField(field);
    }
    public static Selection range(String field, String lower, String upper,boolean includeUpper, boolean includeLower) {
         return new Range(lower, upper, includeUpper, includeLower).setField(field);
    }
    public static Selection range(String field, String lower, String upper) {
        return new Range(lower, upper, true, true).setField(field);
   }
    public static Range range(String field, String from, String to, boolean includeLower, boolean includeUpper, boolean noOptimize, String type) {
      return (Range) new Range(from, to, includeLower, includeUpper, (Double) null, noOptimize).setField(field);
  }
    public static Selection path(String field, String value, boolean strict, int depth) {
        return new Path(value, strict, depth).setField(field);
    }
}
