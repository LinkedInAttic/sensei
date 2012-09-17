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
package com.senseidb.util;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
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
				try {
					val = URLDecoder.decode(val,"UTF-8");
					valList.add(val);
				} catch (UnsupportedEncodingException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
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
