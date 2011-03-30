package com.sensei.indexing.api;

import java.text.DecimalFormat;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.log4j.Logger;
import org.json.JSONObject;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import proj.zoie.api.indexing.AbstractZoieIndexableInterpreter;
import proj.zoie.api.indexing.ZoieIndexable;

import com.sensei.indexing.api.DefaultSenseiInterpreter.IndexSpec;

public class DefaultJsonSchemaInterpreter extends
		AbstractZoieIndexableInterpreter<JSONObject> {

	private static final Logger logger = Logger.getLogger(DefaultJsonSchemaInterpreter.class);
	
	private static class FieldDefinition{
		Format formatter;
		boolean isMeta;
		IndexSpec textIndexSpec;
		String fromField;
	}
	
	private final Map<String,FieldDefinition> _fieldDefMap;
	public DefaultJsonSchemaInterpreter(org.w3c.dom.Document schemaDoc) throws ConfigurationException{
		_fieldDefMap = new HashMap<String,FieldDefinition>();
		NodeList columns = schemaDoc.getElementsByTagName("column");
		for (int i = 0; i < columns.getLength(); ++i) {
			try {
				Element column = (Element) columns.item(i);
				String n = column.getAttribute("name");
				String t = column.getAttribute("type");
				String frm = column.getAttribute("from");
				
				boolean multi = false;
				String multiDelim = null;
				
				if (frm==null){
					frm=n;
				}

				Format formatter = null;
				if (t.equals("int")) {
					MetaType metaType = DefaultSenseiInterpreter.CLASS_METATYPE_MAP.get(int.class);
					String formatString = DefaultSenseiInterpreter.DEFAULT_FORMAT_STRING_MAP.get(metaType);
					formatter = new DecimalFormat(formatString);
				} else if (t.equals("short")) {
					MetaType metaType = DefaultSenseiInterpreter.CLASS_METATYPE_MAP.get(short.class);
					String formatString = DefaultSenseiInterpreter.DEFAULT_FORMAT_STRING_MAP.get(metaType);
					formatter = new DecimalFormat(formatString);
				} else if (t.equals("long")) {
					MetaType metaType = DefaultSenseiInterpreter.CLASS_METATYPE_MAP.get(long.class);
					String formatString = DefaultSenseiInterpreter.DEFAULT_FORMAT_STRING_MAP.get(metaType);
					formatter = new DecimalFormat(formatString);
				} else if (t.equals("float")) {
					MetaType metaType = DefaultSenseiInterpreter.CLASS_METATYPE_MAP.get(float.class);
					String formatString = DefaultSenseiInterpreter.DEFAULT_FORMAT_STRING_MAP.get(metaType);
					formatter = new DecimalFormat(formatString);
				} else if (t.equals("double")) {
					MetaType metaType = DefaultSenseiInterpreter.CLASS_METATYPE_MAP.get(double.class);
					String formatString = DefaultSenseiInterpreter.DEFAULT_FORMAT_STRING_MAP.get(metaType);
					formatter = new DecimalFormat(formatString);
				} else if (t.equals("char")) {
					formatter = null;
				} else if (t.equals("string")) {
					formatter = null;
				} else if (t.equals("boolean")) {
					formatter = null;
				} else if (t.equals("date")) {

					String f = "";
					try {
						f = column.getAttribute("format");
					} catch (Exception ex) {
						logger.error(ex.getMessage(), ex);
					}
					if (f.isEmpty())
						throw new ConfigurationException("Date format cannot be empty.");

					formatter = new SimpleDateFormat(f);
				}
				else if (t.equals("uid")){
					
				}
				else if (t.equals("text")){
					
				}

			} catch (Exception e) {
				throw new ConfigurationException("Error parsing schema: "
						+ columns.item(i), e);
			}
		}
	}
	
	@Override
	public ZoieIndexable convertAndInterpret(JSONObject src) {
		// TODO Auto-generated method stub
		return null;
	}

}
