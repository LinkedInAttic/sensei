package com.senseidb.search.node.impl;

import org.json.JSONObject;

import com.senseidb.search.node.SenseiQueryBuilder;
import com.senseidb.search.node.SenseiQueryBuilderFactory;
import com.senseidb.search.req.SenseiQuery;

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
