package com.senseidb.indexing.activity;

/**
 * Wraps an a List and also provides file persistence support
 *
 */
public interface ActivityValues {
	public void init(int capacity);
	/**
	 * @param index
	 * @param value
	 * @return true, if its advisable to flush changes to disk
	 */
	public boolean update(int index, Object value);
	/**
	 * Deletes the corresponding element
	 * @param index
	 */
	public void delete(int index);
	/**
	 * @return runnable that will flush all pending changes on disk. Runnable is not threadsafe and needs to be executed in the same thread as update and delete methods
	 */
	public Runnable prepareFlush();
	public String getFieldName();
	/**
	 * Closes the corresponding persistence connection
	 */
	public void close();
}