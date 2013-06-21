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
package com.senseidb.indexing;

import org.json.JSONObject;

import com.senseidb.util.JSONUtil.FastJSONArray;
import com.senseidb.util.JSONUtil.FastJSONObject;

public class StringJsonFilter extends DataSourceFilter<String> {

	private JsonFilter _innerFilter;
	
	public StringJsonFilter(){
		_innerFilter = null;
	}
	
	public void setInnerFilter(JsonFilter innerFilter){
		_innerFilter = innerFilter;
	}
	
	public JsonFilter getInnerFilter(){
		return _innerFilter;
	}
	
	@Override
	protected JSONObject doFilter(String data) throws Exception {
		JSONObject json = new FastJSONObject(data);
		if (_innerFilter!=null){
			json = _innerFilter.doFilter(json);
		}
		return json;
	}

}
