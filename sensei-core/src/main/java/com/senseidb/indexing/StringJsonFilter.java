package com.senseidb.indexing;

import org.json.JSONObject;

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
		JSONObject json = new JSONObject(data);
		if (_innerFilter!=null){
			json = _innerFilter.doFilter(json);
		}
		return json;
	}

}
