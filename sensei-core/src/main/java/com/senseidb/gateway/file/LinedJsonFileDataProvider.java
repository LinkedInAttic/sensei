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
package com.senseidb.gateway.file;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;

import org.json.JSONException;
import org.json.JSONObject;

import com.senseidb.indexing.DataSourceFilter;
import com.senseidb.indexing.DataSourceFilterable;
import com.senseidb.util.JSONUtil.FastJSONArray;
import com.senseidb.util.JSONUtil.FastJSONObject;

public class LinedJsonFileDataProvider extends LinedFileDataProvider<JSONObject> implements DataSourceFilterable<String> {

  private DataSourceFilter<String> _dataSourceFilter;

	public LinedJsonFileDataProvider(Comparator<String> versionComparator, File file, long startingOffset) {
		super(versionComparator, file, startingOffset);
	}

  @Override
  public void setFilter(DataSourceFilter<String> filter)
  {
    _dataSourceFilter = filter;
  }

  @Override
  protected JSONObject convertLine(String line) throws IOException {
    try
    {
      if (_dataSourceFilter != null)
        return _dataSourceFilter.filter(line);
    }
    catch(Exception e)
    {
	  throw new IOException(e.getMessage(),e);
    }

    // Try to create directly.
	try {
	  return new FastJSONObject(line);
	} catch (JSONException e) {
	  throw new IOException(e.getMessage(),e);
	}
  }

}
