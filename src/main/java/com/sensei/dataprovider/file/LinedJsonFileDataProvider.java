package com.sensei.dataprovider.file;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;

import org.json.JSONException;
import org.json.JSONObject;

public class LinedJsonFileDataProvider extends LinedFileDataProvider<JSONObject> {

	public LinedJsonFileDataProvider(Comparator<String> versionComparator, File file, long startingOffset) {
		super(versionComparator, file, startingOffset);
	}

	@Override
	protected JSONObject convertLine(String line) throws IOException {
		try {
			return new JSONObject(line);
		} catch (JSONException e) {
			throw new IOException(e.getMessage(),e);
		}
	}

}
