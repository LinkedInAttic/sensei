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
package com.senseidb.search.query;

import org.apache.lucene.search.Query;
import org.json.JSONException;
import org.json.JSONObject;

public class CustomQueryConstructor extends QueryConstructor
{
  public static final String QUERY_TYPE = "custom";

  // "term" : {
  //   "color" : "red"
  //
  //   // or "color" : {"term" : "red", "boost": 2.0}
  // },

  @Override
  protected Query doConstructQuery(JSONObject jsonQuery) throws JSONException
  {
    try
    {
      String className = jsonQuery.getString(CLASS_PARAM);
      Class queryClass = Class.forName(className);

      Object q = queryClass.newInstance();
      //TODO add initialization
      //((SenseiPlugin)q).initialize(jsonQuery.optJSONObject(PARAMS_PARAM));

      ((Query)q).setBoost((float)jsonQuery.optDouble(BOOST_PARAM, 1.0));
      return (Query)q;
    }
    catch(Throwable t)
    {
      throw new IllegalArgumentException(t.getMessage());
    }
  }
}
