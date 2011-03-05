package com.sensei.test;

import java.util.HashSet;
import java.util.Map;

import junit.framework.TestCase;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;

import com.sensei.search.util.RequestConverter;

public class TestRestServer extends TestCase {

	public TestRestServer(String name){
		super(name);
	}
	
	public void testParamParsing() throws Exception{
		BaseConfiguration conf = new BaseConfiguration();
		
		conf.addProperty("select.color.val", "red,blue");
		conf.addProperty("select.color.op", "or");
		conf.addProperty("select.color.prop", "p1:p2");
		
		conf.addProperty("select.city.val", "san jose");
		conf.addProperty("select.city.op", "and");
		conf.addProperty("select.city.prop", "p3:p5");
		

		conf.addProperty("facet.color.minhit", "1");
		conf.addProperty("facet.color.maxcount", "10");
		conf.addProperty("facet.color.expand", "false");
		conf.addProperty("facet.color.order", "hits");
		
		conf.addProperty("facet.category.minhit", "0");
		conf.addProperty("facet.category.maxcount", "5");
		conf.addProperty("facet.category.expand", "true");
		conf.addProperty("facet.category.order", "val");
		
		Map<String,Configuration> mapConf = RequestConverter.parseParamConf(conf, "blah");
		
		assertEquals(0,mapConf.size());
		
		mapConf = RequestConverter.parseParamConf(conf, "select");
		assertEquals(2,mapConf.size());
		
		Configuration selColorConf = mapConf.get("color");
		
		String[] vals = selColorConf.getStringArray("val");
		assertNotNull(vals);
		assertEquals(2, vals.length);
		HashSet<String> targetSet = new HashSet<String>();
		targetSet.add("red");
		targetSet.add("blue");
		for (String val : vals){
			assertTrue(targetSet.remove(val));
		}
		
		Configuration selCityConf = mapConf.get("city");
		
		vals = selCityConf.getStringArray("val");
		assertNotNull(vals);
		assertEquals(1, vals.length);
		assertEquals(vals[0],"san jose");
		
		mapConf = RequestConverter.parseParamConf(conf, "facet");
		assertEquals(2,mapConf.size());
		
		selColorConf = mapConf.get("color");
		assertEquals(false,selColorConf.getBoolean("expand"));
		assertEquals(1,selColorConf.getInt("minhit"));
		assertEquals(10,selColorConf.getInt("maxcount"));
		assertEquals("hits",selColorConf.getString("order"));
		
		selCityConf = mapConf.get("category");
		assertEquals(true,selCityConf.getBoolean("expand"));
		assertEquals(0,selCityConf.getInt("minhit"));
		assertEquals(5,selCityConf.getInt("maxcount"));
		assertEquals("val",selCityConf.getString("order"));
	}
}
