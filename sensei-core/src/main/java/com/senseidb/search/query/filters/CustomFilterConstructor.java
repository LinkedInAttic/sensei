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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Filter;
import org.json.JSONObject;

import java.io.IOException;

public class CustomFilterConstructor extends FilterConstructor
{

  public static final String FILTER_TYPE = "custom";
  
  
//  // custom
//  "custom" : {
//    "class" : "com.sensidb.query.TestFilter"
//  }
  
  @Override
  protected SenseiFilter doConstructFilter(Object json) throws Exception
  {
    try
    {
      String className = ((JSONObject)json).getString(CLASS_PARAM);
      Class filterClass = Class.forName(className);

      final Filter f = (Filter)filterClass.newInstance();

      if(f instanceof SenseiFilter) {
        return (SenseiFilter) f;
      } else {
        return SenseiFilter.buildDefault(f);
      }
    }
    catch(Throwable t)
    {
      throw new IllegalArgumentException(t.getMessage());
    }
  }

}
