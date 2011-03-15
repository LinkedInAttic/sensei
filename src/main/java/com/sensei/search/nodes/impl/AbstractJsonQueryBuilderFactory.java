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
		byte[] bytes = query.toBytes();
		return buildQuery(new JSONObject(new String(bytes,SenseiQuery.utf8Charset)));
	}

	protected abstract SenseiQueryBuilder buildQuery(JSONObject jsonQuery);

}
