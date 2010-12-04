package com.sensei.search.nodes.impl;

import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;

import com.sensei.avro.SenseiAvroQuery;
import com.sensei.search.nodes.SenseiQueryBuilder;

public class DefaultAvroQueryBuilderFactory extends
		AbstractAvroQueryBuilderFactory<SenseiAvroQuery> {

	private final QueryParser _qparser;
	public DefaultAvroQueryBuilderFactory(QueryParser qparser) {
		super(SenseiAvroQuery.class);
		_qparser = qparser;
	}

	@Override
	protected SenseiQueryBuilder buildQuery(SenseiAvroQuery avroQuery) {
		CharSequence charSeq = avroQuery == null ? null : avroQuery.query;
		final String queryString;
		if (charSeq!=null && charSeq.length()>0){
			queryString = charSeq.toString();
		}
		else{
			queryString = null;
		}
		
		return new SenseiQueryBuilder(){

			@Override
			public Filter buildFilter() throws ParseException {
				return null;
			}

			@Override
			public Query buildQuery() throws ParseException {
				if (queryString!=null){
					return _qparser.parse(queryString);
				}
				else{
					return new MatchAllDocsQuery();
				}
			}
			
		};
	}

}
