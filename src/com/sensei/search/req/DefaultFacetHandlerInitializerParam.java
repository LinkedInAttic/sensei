package com.sensei.search.req;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DefaultFacetHandlerInitializerParam implements
		FacetHandlerInitializerParam,Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private final Map<String,boolean[]> _boolMap;
	private final Map<String,int[]> _intMap;
	private final Map<String,long[]> _longMap;
	private final Map<String,List<String>> _stringMap;
	private final Map<String,byte[]> _byteMap;
	private final Map<String,double[]> _doubleMap;
	
	public DefaultFacetHandlerInitializerParam(){
		_boolMap = new HashMap<String,boolean[]>();
		_intMap = new HashMap<String,int[]>();
		_longMap = new HashMap<String,long[]>();
		_stringMap = new HashMap<String,List<String>>();
		_byteMap = new HashMap<String,byte[]>();
		_doubleMap = new HashMap<String,double[]>();
	}
	
	public Set<String> getBooleanParamNames(){
		return _boolMap.keySet();
	}
	
	public Set<String> getStringParamNames(){
		return _stringMap.keySet();
	}
	
	public Set<String> getIntParamNames(){
		return _intMap.keySet();
	}
	
	public Set<String> getByteArrayParamNames(){
		return _byteMap.keySet();
	}
	
	public Set<String> getLongParamNames(){
		return _longMap.keySet();
	}
	
	public Set<String> getDoubleParamNames(){
		return _doubleMap.keySet();
	}
	
	public void putBooleanParam(String key,boolean[] value){
		_boolMap.put(key, value);
	}

	public boolean[] getBooleanParam(String name) {
		return _boolMap.get(name);
	}

	public void putByteArrayParam(String key,byte[] value){
		_byteMap.put(key, value);
	}
	
	public byte[] getByteArrayParam(String name) {
		return _byteMap.get(name);
	}

	public void putIntParam(String key,int[] value){
		_intMap.put(key, value);
	}
	
	public int[] getIntParam(String name) {
		return _intMap.get(name);
	}

	public void putLongParam(String key,long[] value){
		_longMap.put(key, value);
	}
	
	public long[] getLongParam(String name) {
		return _longMap.get(name);
	}

	public void putStringParam(String key,List<String> value){
		_stringMap.put(key, value);
	}
	
	public List<String> getStringParam(String name) {
		return _stringMap.get(name);
	}

	public void putDoubleParam(String key,double[] value){
		_doubleMap.put(key, value);
	}
	
	public double[] getDoubleParam(String name) {
		return _doubleMap.get(name);
	}
	
	public void clear(){
		_boolMap.clear();
		_intMap.clear();
		_longMap.clear();
		_stringMap.clear();
		_byteMap.clear();
	}

}
