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
package com.senseidb.search.query.filters;

import java.util.Iterator;

import org.apache.lucene.search.Filter;
import org.json.JSONObject;


public class TermFilterConstructor extends FilterConstructor{
  public static final String FILTER_TYPE = "term";

  @Override
  protected Filter doConstructFilter(Object param) throws Exception {
    JSONObject json = (JSONObject)param;

    Iterator<String> iter = json.keys();
    if (!iter.hasNext())
      return null;

    String field = iter.next();
    String text;
    boolean noOptimize = false;

    if (NOOPTIMIZE_PARAM.equals(field))
    {
      noOptimize = json.optBoolean(NOOPTIMIZE_PARAM, false);
      field = iter.next();
    }

    Object obj = json.get(field);
    if (obj instanceof JSONObject)
    {
      text = ((JSONObject)obj).getString(VALUE_PARAM);
      noOptimize = json.optBoolean(NOOPTIMIZE_PARAM, false);
    }
    else
    {
      text = String.valueOf(obj);
    }

    return new SenseiTermFilter(field, new String[]{text}, null, false, noOptimize);
  }
  
}
