package com.sensei.search.req;

import com.sensei.search.util.AvroSerializerHelper;
import java.io.Serializable;

public class AvroQuery<V> extends SenseiQuery implements Serializable {

	private static final long serialVersionUID = 1L;

	public AvroQuery(V v,Class<V> cls) throws Exception{
		super(AvroSerializerHelper.toBytes(v, cls));
	}	
}
