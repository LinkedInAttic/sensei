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

package com.senseidb.test.client;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.json.JSONObject;

import com.senseidb.search.client.SenseiServiceProxy;
import com.senseidb.search.client.json.JsonSerializer;
import com.senseidb.search.client.req.SenseiClientRequest;
import com.senseidb.search.client.req.Sort;
import com.senseidb.search.client.req.query.Queries;
import com.senseidb.search.client.req.relevance.Model;
import com.senseidb.search.client.req.relevance.Relevance;
import com.senseidb.search.client.req.relevance.RelevanceFacetType;
import com.senseidb.search.client.req.relevance.RelevanceValues;
import com.senseidb.search.client.req.relevance.VariableType;

public class RelevanceExample {
public static void main(String[] args) throws Exception {
  SenseiServiceProxy senseiServiceProxy = new SenseiServiceProxy("localhost", 8080);
  Model model = Model.builder().addFacets(RelevanceFacetType.type_int, "year","mileage").
      addFacets(RelevanceFacetType.type_long, "groupid").addFacets(RelevanceFacetType.type_string, "color","category").
      addFunctionParams("_INNER_SCORE", "thisYear", "year","goodYear","mileageWeight","mileage","color", "yearcolor", "colorweight", "category", "categorycolor").
      addVariables(VariableType.set_int, "goodYear").addVariables(VariableType.type_int, "thisYear").
      addVariables(VariableType.map_int_float, "mileageWeight").addVariables(VariableType.map_int_string, "yearcolor")
      .addVariables(VariableType.map_string_float, "colorweight").addVariables(VariableType.map_string_string, "categorycolor").
      function(" if(categorycolor.containsKey(category) && categorycolor.get(category).equals(color))  return 10000f; if(colorweight.containsKey(color) ) return 200f + colorweight.getFloat(color); if(yearcolor.containsKey(year) && yearcolor.get(year).equals(color)) return 200f; if(mileageWeight.containsKey(mileage)) return 10000+mileageWeight.get(mileage); if(goodYear.contains(year)) return (float)Math.exp(2d);   if(year==thisYear) return 87f   ; return  _INNER_SCORE;").build();
  Map<Object, Object> map = new HashMap<Object, Object>();
  map.put("red", 335.5);
  RelevanceValues.RelevanceValuesBuilder valuesBuilder = new RelevanceValues.RelevanceValuesBuilder().addAtomicValue("thisYear", 2001)
      .addListValue("goodYear", 1996,1997).
      addMapValue("mileageWeight", Arrays.asList(11400,11000), Arrays.asList(777.9, 10.2))
      .addMapValue("colorweight", map);
  map.clear();
  map.put(1998, "red");
  valuesBuilder.addMapValue("yearcolor", map);
  valuesBuilder.addMapValue("categorycolor", Arrays.asList("compact"), Arrays.asList("white"));
  SenseiClientRequest request = SenseiClientRequest.builder().addSort(Sort.byRelevance()).query(Queries.stringQuery("").setRelevance(Relevance.valueOf(model, valuesBuilder.build()))).showOnlyFields("color").build();
  System.out.println(((JSONObject)JsonSerializer.serialize(request)).toString(1));
  System.out.println(senseiServiceProxy.sendSearchRequest(request));
}
}
