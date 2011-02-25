package com.sensei.search.req;

import org.json.JSONObject;

public class SenseiJSONQuery extends SenseiQuery {
	
	private static final long serialVersionUID = 1L;
	
	public SenseiJSONQuery(JSONObject jsonObj){
		super(jsonObj.toString().getBytes(SenseiQuery.utf8Charset));
	}
}
