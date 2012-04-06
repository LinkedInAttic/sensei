package com.senseidb.indexing.activity;

public interface ActivityValues {
	public void init(int capacity);
	public boolean update(int index, Object value);
	public void delete(int index);
	public Runnable prepareFlush();
	public String getFieldName();
	public void close();
}