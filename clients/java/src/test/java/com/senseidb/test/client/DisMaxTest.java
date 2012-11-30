package com.senseidb.test.client;

import com.google.common.collect.Lists;
import com.senseidb.search.client.req.Term;
import com.senseidb.search.client.req.query.DisMax;


import junit.framework.TestCase;

public class DisMaxTest extends TestCase {
    
    public void testConstruction(){
	new DisMax(1.0, Lists.<Term>newArrayList(), 1.0);
    }
    
    public void testToString(){
	DisMax query = new DisMax(1.0, Lists.<Term>newArrayList(), 1.0);
	query.toString();
    }

}
