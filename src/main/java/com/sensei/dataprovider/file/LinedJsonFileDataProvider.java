package com.sensei.dataprovider.file;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;

import org.json.JSONException;
import org.json.JSONObject;

import com.sensei.indexing.api.DataSourceFilter;
import com.sensei.indexing.api.DataSourceFilterable;

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
			return new JSONObject(line);
		} catch (JSONException e) {
			throw new IOException(e.getMessage(),e);
		}
	}

}
