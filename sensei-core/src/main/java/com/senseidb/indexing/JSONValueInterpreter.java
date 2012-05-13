package com.senseidb.indexing;

import org.json.JSONObject;

import com.linkedin.zoie.api.indexing.AbstractZoieIndexableInterpreter;
import com.linkedin.zoie.api.indexing.ZoieIndexable;

public abstract class JSONValueInterpreter<V> extends
		AbstractZoieIndexableInterpreter<JSONObject> {
	
	private final DefaultSenseiInterpreter<V> _innerInterpreter;
	
	public JSONValueInterpreter(Class<V> cls){
		_innerInterpreter = new DefaultSenseiInterpreter<V>(cls);
	}

	public abstract V buildDataObj(JSONObject jsonObj);
	
	@Override
	public ZoieIndexable convertAndInterpret(JSONObject src) {
		V obj = buildDataObj(src);
		return _innerInterpreter.convertAndInterpret(obj);
	}
}
