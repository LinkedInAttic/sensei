package com.sensei.indexing.api;

import org.json.JSONObject;

public interface ShardingStrategy {
	int caculateShard(int maxShardId,long uid,JSONObject dataObj);
	
	public static class UidModShardingStrategy implements ShardingStrategy{

		@Override
		public int caculateShard(int maxShardId, long uid, JSONObject dataObj) {
			return (int)(uid % maxShardId);
		}
	}
}
