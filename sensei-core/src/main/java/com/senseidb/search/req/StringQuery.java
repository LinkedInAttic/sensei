package com.senseidb.search.req;

final public class StringQuery extends SenseiQuery {
	public StringQuery(String q){
		super(q.getBytes(UTF_8_CHARSET));
	}

}
