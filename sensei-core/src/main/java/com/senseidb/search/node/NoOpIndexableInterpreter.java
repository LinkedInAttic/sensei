package com.senseidb.search.node;

import com.linkedin.zoie.api.indexing.AbstractZoieIndexableInterpreter;
import com.linkedin.zoie.api.indexing.ZoieIndexable;


public class NoOpIndexableInterpreter extends
		AbstractZoieIndexableInterpreter<ZoieIndexable> {

	@Override
	public ZoieIndexable convertAndInterpret(ZoieIndexable obj) {
		return obj;
	}
}
