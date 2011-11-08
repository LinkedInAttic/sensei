package com.sensei.indexing.hadoop.demo;

import org.json.JSONException;
import org.json.JSONObject;

import com.sensei.indexing.api.ShardingStrategy;


public class CarShardingStrategy implements ShardingStrategy {

	@Override
	public int caculateShard(int maxShardId, JSONObject json)
			throws JSONException {
		return (json.getInt("id")) % maxShardId ;
	}

}
