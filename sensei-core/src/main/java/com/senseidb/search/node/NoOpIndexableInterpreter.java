package com.senseidb.search.node;

import proj.zoie.api.indexing.AbstractZoieIndexableInterpreter;
import proj.zoie.api.indexing.ZoieIndexable;


public class NoOpIndexableInterpreter extends
		AbstractZoieIndexableInterpreter<ZoieIndexable> {

	@Override
	public ZoieIndexable convertAndInterpret(ZoieIndexable obj) {
		return obj;
	}
}
