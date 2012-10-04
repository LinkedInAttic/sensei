/**
 * This software is licensed to you under the Apache License, Version 2.0 (the
 * "Apache License").
 *
 * LinkedIn's contributions are made under the Apache License. If you contribute
 * to the Software, the contributions will be deemed to have been made under the
 * Apache License, unless you expressly indicate otherwise. Please do not make any
 * contributions that would be inconsistent with the Apache License.
 *
 * You may obtain a copy of the Apache License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, this software
 * distributed under the Apache License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the Apache
 * License for the specific language governing permissions and limitations for the
 * software governed under the Apache License.
 *
 * © 2012 LinkedIn Corp. All Rights Reserved.  
 */
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
