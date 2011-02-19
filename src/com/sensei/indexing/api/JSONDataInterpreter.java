package com.sensei.indexing.api;

import org.apache.lucene.document.Document;
import org.json.JSONObject;

import proj.zoie.api.indexing.AbstractZoieIndexableInterpreter;
import proj.zoie.api.indexing.ZoieIndexable;

public abstract class JSONDataInterpreter extends AbstractZoieIndexableInterpreter<JSONObject> {

	@Override
	public ZoieIndexable convertAndInterpret(final JSONObject src) {
		return new ZoieIndexable(){

			@Override
			public IndexingReq[] buildIndexingReqs() {
				return new IndexingReq[]{new IndexingReq(buildDoc(src))};
			}

			@Override
			public long getUID() {
				return extractUID(src);
			}

			@Override
			public boolean isDeleted() {
				return extractDeleteFlag(src);
			}

			@Override
			public boolean isSkip() {
				return extractSkipFlag(src);
			}
			
		};
	}

	public abstract long extractUID(JSONObject obj);
	public abstract Document buildDoc(JSONObject obj);
	
	public boolean extractSkipFlag(JSONObject obj){
		return false;
	}
	
	public boolean extractDeleteFlag(JSONObject obj){
		return false;
	}
}
