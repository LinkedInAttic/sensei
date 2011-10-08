package com.sensei.indexing.hadoop.util;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;

public class PropertiesLoader {
	
	public static void loadProperties(Configuration conf, Properties properties) {
		Iterator itr = properties.keySet().iterator(); 
		while(itr.hasNext()) { 
		  String key = (String) itr.next(); 
		  Object v = properties.getProperty(key);
		  if(v instanceof String)
			conf.set(key, (String) v); 
		  else if(v instanceof Boolean)
			conf.setBoolean(key, (Boolean)v);
		  else if(v instanceof Float)
			conf.setFloat(key, (Float)v);
		  else if(v instanceof Integer)
			conf.setInt(key, (Integer)v);
		  else if(v instanceof Long)
			conf.setLong(key, (Long)v);
		} 
	  }

	public static Configuration loadProperties(String path) throws IOException{
		InputStream inputStream = new BufferedInputStream(new FileInputStream(new File(path).getAbsolutePath()));
	    Properties properties = new Properties();
	    properties.load(inputStream);
		Configuration conf = new Configuration();
		loadProperties(conf, properties);
		return conf;
	}
}
