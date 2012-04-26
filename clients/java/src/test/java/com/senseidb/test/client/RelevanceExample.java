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
