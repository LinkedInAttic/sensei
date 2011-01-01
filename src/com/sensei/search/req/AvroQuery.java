package com.sensei.search.req;

import com.sensei.search.util.AvroSerializerHelper;
import java.io.Serializable;

public class AvroQuery<V> extends SenseiQuery implements Serializable {

	private final String _stringForm;
	
	public AvroQuery(V v,Class<V> cls) throws Exception{
		super(AvroSerializerHelper.toBytes(v, cls));
		_stringForm = v.toString();
	}

	@Override
	public String toString() {
		return _stringForm;
	}	
}
