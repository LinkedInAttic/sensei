package com.sensei.indexing.api;

import java.lang.reflect.Method;

import org.apache.log4j.Logger;

import proj.zoie.api.indexing.ZoieIndexable;

public class DefaultSenseiZoieIndexable<V> implements ZoieIndexable {

	private static final Logger logger = Logger.getLogger(DefaultSenseiZoieIndexable.class);
	
	private final V _obj;
	private final DefaultSenseiInterpreter<V> _interpreter;
	private final Class<V> _cls;
	
	DefaultSenseiZoieIndexable(V obj,Class<V> cls,DefaultSenseiInterpreter<V> interpreter){
		_obj = obj;
		_interpreter = interpreter;
		_cls = cls;
	}
	
	@Override
	public IndexingReq[] buildIndexingReqs() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public long getUID() {
		try {
			return _interpreter._uidField.getLong(_obj);
		} catch (Exception e) {
			throw new RuntimeException(e.getMessage(),e);
		}
	}
	
	private boolean checkViaReflection(Method m){
		boolean retVal = false;
		if (m!=null){
			try {
				Object retObj = m.invoke(_obj, null);
				retVal = ((Boolean)retObj).booleanValue();
			} catch (Exception e) {
				throw new RuntimeException(e.getMessage(),e);
			}
		}
		return retVal;
	}

	@Override
	public boolean isDeleted() {
		return checkViaReflection(_interpreter._deleteChecker);
	}

	@Override
	public boolean isSkip() {

		return checkViaReflection(_interpreter._skipChecker);
	}

}
