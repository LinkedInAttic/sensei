package com.sensei.search.nodes.impl;

import org.apache.lucene.queryParser.QueryParser;

import com.sensei.search.nodes.SenseiQueryBuilder;
import com.sensei.search.nodes.SenseiQueryBuilderFactory;
import com.sensei.search.req.SenseiQuery;
import com.sensei.search.util.AvroSerializerHelper;

public abstract class AbstractAvroQueryBuilderFactory<V> implements
		SenseiQueryBuilderFactory {

	private final Class<V> _cls;
	private final QueryParser _qparser;
	public AbstractAvroQueryBuilderFactory(QueryParser qparser,Class<V> cls){
		_cls = cls;
		_qparser = qparser;
	}
	
	public QueryParser getQueryParser(){
		return _qparser;
	}
	
	@Override
	public SenseiQueryBuilder getQueryBuilder(SenseiQuery query)
			throws Exception {
		byte[] bytes = query.toBytes();
		
		V avroQuery = null;
		
		if (bytes!=null && bytes.length>0){
		  avroQuery = AvroSerializerHelper.fromBytes(bytes, _cls);
		}
		return buildQuery(avroQuery);
	}

	protected abstract SenseiQueryBuilder buildQuery(V avroQuery);
}
