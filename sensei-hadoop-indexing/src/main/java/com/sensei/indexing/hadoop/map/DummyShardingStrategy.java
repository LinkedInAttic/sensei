package com.sensei.indexing.hadoop.map;

import org.json.JSONException;
import org.json.JSONObject;

import com.sensei.indexing.api.ShardingStrategy;


public class DummyShardingStrategy implements ShardingStrategy {

	@Override
	public int caculateShard(int maxShardId, JSONObject dataObj)
			throws JSONException {
		return dataObj.toString().length() % maxShardId;
	}

}
