/**
 * This software is licensed to you under the Apache License, Version 2.0 (the
 * "Apache License").
 *
 * LinkedIn's contributions are made under the Apache License. If you contribute
 * to the Software, the contributions will be deemed to have been made under the
 * Apache License, unless you expressly indicate otherwise. Please do not make any
 * contributions that would be inconsistent with the Apache License.
 *
 * You may obtain a copy of the Apache License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, this software
 * distributed under the Apache License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the Apache
 * License for the specific language governing permissions and limitations for the
 * software governed under the Apache License.
 *
 * © 2012 LinkedIn Corp. All Rights Reserved.  
 */
package com.senseidb.indexing.hadoop.util;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;

public class PropertiesLoader {
	
	public static void loadProperties(Configuration conf, Properties properties) {
		for (Entry<Object, Object> entry : properties.entrySet()) {
			String key = (String) entry.getKey();
			Object v = entry.getValue();
			if (v instanceof String)
				conf.set(key, (String) v);
			else if (v instanceof Boolean)
				conf.setBoolean(key, (Boolean) v);
			else if (v instanceof Float)
				conf.setFloat(key, (Float) v);
			else if (v instanceof Integer)
				conf.setInt(key, (Integer) v);
			else if (v instanceof Long)
				conf.setLong(key, (Long) v);
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
