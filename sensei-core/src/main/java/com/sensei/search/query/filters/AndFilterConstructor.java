package com.sensei.search.query.filters;

import java.io.IOException;
import java.util.Iterator;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Filter;
import org.json.JSONObject;

import com.sensei.search.query.FilterConstructor;

public class AndFilterConstructor extends FilterConstructor {

	@Override
	public Filter constructFilter(final JSONObject json) throws Exception {
	   return null;
		
	}

}
