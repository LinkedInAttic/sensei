package com.sensei.search.req;

import java.util.List;
import java.util.Set;


public interface FacetHandlerInitializerParam{
	List<String> getStringParam(String name);
	int[] getIntParam(String name);
	boolean[] getBooleanParam(String name);
	long[] getLongParam(String name);
	byte[] getByteArrayParam(String name);
	double[] getDoubleParam(String name);
	Set<String> getBooleanParamNames();
	Set<String> getStringParamNames();
	Set<String> getIntParamNames();
	Set<String> getByteArrayParamNames();
	Set<String> getLongParamNames();
	Set<String> getDoubleParamNames();
}
