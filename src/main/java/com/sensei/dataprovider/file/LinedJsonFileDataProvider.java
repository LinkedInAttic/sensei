package com.sensei.dataprovider.file;

import java.io.File;
import java.io.IOException;

import org.json.JSONException;
import org.json.JSONObject;

public class LinedJsonFileDataProvider extends LinedFileDataProvider<JSONObject> {

	public LinedJsonFileDataProvider(File file, long startingOffset) {
		super(file, startingOffset);
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
