package com.sensei.search.nodes.impl;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Filter;

import com.browseengine.bobo.api.BoboIndexReader;
import com.browseengine.bobo.facets.FacetHandler;
import com.browseengine.bobo.facets.filter.RandomAccessFilter;

public class SenseiTimeFilter extends Filter {

	private static final long serialVersionUID = 1L;
	private final String _timeField;
	private final TimeUnit _timeUnit;
	private final int _numDays;
	
	public SenseiTimeFilter(String timeField,TimeUnit timeUnit,int numDays){
		_timeField = timeField;
		_timeUnit = timeUnit;
		_numDays = numDays;
	}
	
	public static long buildFromTime(int numDays,TimeUnit timeUnit){
		 long time = System.currentTimeMillis();
		 long convertedNow=timeUnit.convert(time, TimeUnit.MILLISECONDS);
		 long converted = timeUnit.convert(numDays, TimeUnit.DAYS);
		 return convertedNow - converted;
	}
	
	private static String buildTimeRangeString(int numDays,TimeUnit timeUnit){
		
		 long from = buildFromTime(numDays,timeUnit);
		 StringBuilder buf = new StringBuilder();
		 buf.append("[").append(from).append(" TO *]");
		 return buf.toString();
	}
	
	@Override
	public DocIdSet getDocIdSet(IndexReader reader) throws IOException {
		if (reader instanceof BoboIndexReader){
		    BoboIndexReader boboReader = (BoboIndexReader)reader;
		    FacetHandler<?> timeFacetHandler = boboReader.getFacetHandler(_timeField);
		    if (timeFacetHandler==null){
		    	throw new IOException(_timeField+" not defined");
		    }
		    
		    String rangeString = buildTimeRangeString(_numDays,_timeUnit);
		    RandomAccessFilter filter = timeFacetHandler.buildRandomAccessFilter(rangeString, null);
		    
		    return filter.getDocIdSet(reader);
		}
		else{
			throw new IOException("reader not instance of "+BoboIndexReader.class);
		}
	}
}
