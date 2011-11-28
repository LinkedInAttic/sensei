package com.sensei.search.nodes.impl;

import org.json.JSONObject;

import com.sensei.search.nodes.SenseiQueryBuilder;
import com.sensei.search.nodes.SenseiQueryBuilderFactory;
import com.sensei.search.req.SenseiQuery;

public abstract class AbstractJsonQueryBuilderFactory implements
		SenseiQueryBuilderFactory {

	@Override
	public SenseiQueryBuilder getQueryBuilder(SenseiQuery query)
			throws Exception {
		JSONObject jsonQuery=null;
		if (query!=null){
			byte[] bytes = query.toBytes();
			jsonQuery = new JSONObject(new String(bytes,SenseiQuery.utf8Charset));
		}
		return buildQueryBuilder(jsonQuery);
	}

	public abstract SenseiQueryBuilder buildQueryBuilder(JSONObject jsonQuery);

}
