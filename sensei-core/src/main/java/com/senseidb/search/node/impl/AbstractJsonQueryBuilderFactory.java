package com.senseidb.search.node.impl;

import org.apache.commons.lang.StringUtils;
import org.json.JSONObject;

import com.senseidb.search.node.SenseiQueryBuilder;
import com.senseidb.search.node.SenseiQueryBuilderFactory;
import com.senseidb.search.req.SenseiQuery;
import com.senseidb.util.JSONUtil.FastJSONArray;
import com.senseidb.util.JSONUtil.FastJSONObject;

public abstract class AbstractJsonQueryBuilderFactory implements
		SenseiQueryBuilderFactory {

	@Override
	public SenseiQueryBuilder getQueryBuilder(SenseiQuery query)
			throws Exception {
		JSONObject jsonQuery=null;
        String queryString = query.toString();
		if (!StringUtils.isEmpty(queryString)){
//			byte[] bytes = query.toBytes();
//			jsonQuery = new FastJSONObject(new String(bytes,SenseiQuery.utf8Charset));
            jsonQuery = new FastJSONObject(queryString);
		}
		return buildQueryBuilder(jsonQuery);
	}

	public abstract SenseiQueryBuilder buildQueryBuilder(JSONObject jsonQuery);

}
