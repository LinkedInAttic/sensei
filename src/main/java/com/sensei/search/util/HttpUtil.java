package com.sensei.search.util;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class HttpUtil {

	public static Map<String,List<String>> buildRequestMap(String reqString){
		Map<String,List<String>> map = new HashMap<String,List<String>>();
		
		String[] params = reqString.split("&");
		for (String param : params){
			String[] parts = param.split("=");
			if (parts.length != 2) continue;
			
			String key = parts[0];
			String val = parts[1];
			
			if (val!=null && val.length()>0){
				List<String> valList = map.get(key);
				if (valList == null){
					valList = new LinkedList<String>();
					map.put(key, valList);
				}
				valList.add(val);
			}
		}
		
		return map;
	}
	
	public static void main(String[] args) {
		String reqstring ="q=cool&start=0&rows=10&facet.city.expand=true&facet.city.minhit=1&facet.city.max=10&facet.city.order=hits&facet.makemodel.expand=true&facet.makemodel.minhit=1&facet.makemodel.max=10&facet.makemodel.order=hits&facet.tags.expand=false&facet.tags.minhit=1&facet.tags.max=10&facet.tags.order=hits&facet.color.expand=true&facet.color.minhit=1&facet.color.max=10&facet.color.order=hits&facet.category.expand=true&facet.category.minhit=1&facet.category.max=10&facet.category.order=hits&facet.year.expand=true&facet.year.minhit=1&facet.year.max=10&facet.year.order=hits&facet.price.expand=true&facet.price.minhit=1&facet.price.max=10&facet.price.order=hits&facet.mileage.expand=true&facet.mileage.minhit=1&facet.mileage.max=10&facet.mileage.order=hits&sort=relevance";
		
		Map map = buildRequestMap(reqstring);
		
		System.out.println(map);
	}
}
