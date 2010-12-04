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

	public DefaultAvroQueryBuilderFactory(QueryParser qparser) {
		super(qparser, SenseiAvroQuery.class);
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
		
		final QueryParser qparser = getQueryParser();
		
		return new SenseiQueryBuilder(){

			@Override
			public Filter buildFilter() throws ParseException {
				return null;
			}

			@Override
			public Query buildQuery() throws ParseException {
				if (queryString!=null){
					return qparser.parse(queryString);
				}
				else{
					return new MatchAllDocsQuery();
				}
			}
			
		};
	}

}
