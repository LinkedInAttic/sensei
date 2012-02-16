package com.senseidb.conf;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.log4j.Logger;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.Field.TermVector;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import com.senseidb.indexing.DefaultSenseiInterpreter;
import com.senseidb.indexing.DefaultSenseiInterpreter.IndexSpec;
import com.senseidb.indexing.MetaType;

public class SenseiSchema {
  public static final String SRC_DATA_FIELD_NAME            = "__SRC_DATA__";
  public static final String SRC_DATA_COMPRESSED_FIELD_NAME = "stored";

  public static final String EVENT_TYPE_FIELD  = "type";
  public static final String EVENT_FIELD       = "data";
  public static final String EVENT_TYPE_ADD    = "add";
  public static final String EVENT_TYPE_UPDATE = "update";
  public static final String EVENT_TYPE_DELETE = "delete";
  public static final String EVENT_TYPE_SKIP   = "skip";

	private static Logger logger = Logger.getLogger(SenseiSchema.class);
	
	private String _uidField;
	private String _deleteField;
	private String _skipField;
	private String _srcDataStore;
	private String _srcDataField;
  private boolean _compressSrcData;
	
	public static class FieldDefinition{
		public Format formatter;
		public boolean isMeta;
		public IndexSpec textIndexSpec;
		public String fromField;
		public boolean isMulti;
		public String delim = ",";
		public Class type = null;
	}
	
	private SenseiSchema(){
		
	}
	
	public String getUidField(){
		return _uidField;
	}


	public String getDeleteField(){
		return _deleteField;
	}
	
	public String getSkipField(){
		return _skipField;
	}

	public String getSrcDataField(){
		return _srcDataField;
	}

	public String getSrcDataStore(){
		return _srcDataStore;
	}

	public boolean isCompressSrcData(){
		return _compressSrcData;
	}

	public Map<String,FieldDefinition> getFieldDefMap(){
		return _fieldDefMap;
	}
	
	private Map<String,FieldDefinition> _fieldDefMap;
	
	public static SenseiSchema build(JSONObject schemaObj) throws JSONException,ConfigurationException{
	  SenseiSchema schema = new SenseiSchema();
      schema._fieldDefMap = new HashMap<String,FieldDefinition>();
      JSONObject tableElem = schemaObj.optJSONObject("table");
      if (tableElem==null){
          throw new ConfigurationException("empty schema");
      }
      
      schema._uidField = tableElem.getString("uid");
      schema._deleteField = tableElem.optString("delete-field","");
      schema._skipField = tableElem.optString("skip-field","");
      schema._srcDataStore = tableElem.optString("src-data-store","");
      schema._srcDataField = tableElem.optString("src-data-field","src_data");
      schema._compressSrcData = tableElem.optBoolean("compress-src-data",true);
      
      JSONArray columns = tableElem.optJSONArray("columns");

      int count = 0;
      if (columns!=null){
         count = columns.length();
      }
    
      for (int i = 0; i < count; ++i) {

        JSONObject column = columns.getJSONObject(i);  
        try {
              String n = column.getString("name");
              String t = column.getString("type");
              String frm = column.optString("from");
              
              FieldDefinition fdef = new FieldDefinition();
              fdef.formatter = null;
              fdef.fromField = frm.length() > 0 ? frm : n;

              fdef.isMeta = true;
              
              fdef.isMulti = column.optBoolean("multi");
              
              
              String delimString = column.optString("delimiter");
              if (delimString!=null && delimString.trim().length()>0){
                  fdef.delim = delimString;
              }
              
              schema._fieldDefMap.put(n, fdef);
              
              if (t.equals("int")) {
                  MetaType metaType = DefaultSenseiInterpreter.CLASS_METATYPE_MAP.get(int.class);
                  String formatString = DefaultSenseiInterpreter.DEFAULT_FORMAT_STRING_MAP.get(metaType);
                  fdef.formatter = new DecimalFormat(formatString, new DecimalFormatSymbols(Locale.US));
                  fdef.type = int.class;
              } else if (t.equals("short")) {
                  MetaType metaType = DefaultSenseiInterpreter.CLASS_METATYPE_MAP.get(short.class);
                  String formatString = DefaultSenseiInterpreter.DEFAULT_FORMAT_STRING_MAP.get(metaType);
                  fdef.formatter = new DecimalFormat(formatString, new DecimalFormatSymbols(Locale.US));
                  fdef.type = int.class;
              } else if (t.equals("long")) {
                  MetaType metaType = DefaultSenseiInterpreter.CLASS_METATYPE_MAP.get(long.class);
                  String formatString = DefaultSenseiInterpreter.DEFAULT_FORMAT_STRING_MAP.get(metaType);
                  fdef.formatter = new DecimalFormat(formatString, new DecimalFormatSymbols(Locale.US));
                  fdef.type = long.class;
              } else if (t.equals("float")) {
                  MetaType metaType = DefaultSenseiInterpreter.CLASS_METATYPE_MAP.get(float.class);
                  String formatString = DefaultSenseiInterpreter.DEFAULT_FORMAT_STRING_MAP.get(metaType);
                  fdef.formatter = new DecimalFormat(formatString, new DecimalFormatSymbols(Locale.US));
                  fdef.type = double.class;
              } else if (t.equals("double")) {
                  MetaType metaType = DefaultSenseiInterpreter.CLASS_METATYPE_MAP.get(double.class);
                  String formatString = DefaultSenseiInterpreter.DEFAULT_FORMAT_STRING_MAP.get(metaType);
                  fdef.formatter = new DecimalFormat(formatString, new DecimalFormatSymbols(Locale.US));
                  fdef.type = double.class;
              } else if (t.equals("char")) {
                  fdef.formatter = null;
              } else if (t.equals("string")) {
                  fdef.formatter = null;
              } else if (t.equals("boolean")) {
                  fdef.formatter = null;
              } else if (t.equals("date")) {

                  String f = "";
                  try {
                      f = column.optString("format");
                  } catch (Exception ex) {
                      logger.error(ex.getMessage(), ex);
                  }
                  if (f.isEmpty())
                      throw new ConfigurationException("Date format cannot be empty.");

                  fdef.formatter = new SimpleDateFormat(f);
                  fdef.type = Date.class;
              }
              else if (t.equals("text")){
                  fdef.isMeta = false;
                  String idxString = column.optString("index", null);
                  String storeString = column.optString("store", null);
                  String tvString = column.optString("termvector", null);
                  Index idx = idxString == null ? Index.ANALYZED : DefaultSenseiInterpreter.INDEX_VAL_MAP.get(idxString.toUpperCase());
                  Store store = storeString == null ? Store.NO : DefaultSenseiInterpreter.STORE_VAL_MAP.get(storeString.toUpperCase());
                  TermVector tv = tvString == null ? TermVector.NO : DefaultSenseiInterpreter.TV_VAL_MAP.get(tvString.toUpperCase());
                  
                  if (idx==null || store==null || tv==null){
                    throw new ConfigurationException("Invalid indexing parameter specification");
                  }
                  
                  IndexSpec indexingSpec = new IndexSpec();
                  indexingSpec.store = store;
                  indexingSpec.index = idx;
                  indexingSpec.tv = tv;
                    
                  fdef.textIndexSpec = indexingSpec; 
              }

          } catch (Exception e) {
              throw new ConfigurationException("Error parsing schema: "
                      + column, e);
          }
      }
      return schema;
	}

	public static SenseiSchema build(Document schemaDoc) throws ConfigurationException{
		SenseiSchema schema = new SenseiSchema();
		schema._fieldDefMap = new HashMap<String,FieldDefinition>();
		NodeList tables = schemaDoc.getElementsByTagName("table");
		if (tables==null || tables.getLength()==0){
			throw new ConfigurationException("empty schema");
		}
		if (tables.getLength()>1){
			throw new ConfigurationException("multiple schemas not supported");
		}
		
		Element tableElem = (Element) tables.item(0);
		schema._uidField = tableElem.getAttribute("uid");
		schema._deleteField = tableElem.getAttribute("delete-field");
		if (schema._deleteField==null) schema._deleteField="";
		schema._skipField = tableElem.getAttribute("skip-field");
		if (schema._skipField==null) schema._skipField="";
		schema._srcDataStore = tableElem.getAttribute("src-data-store");
		if (schema._srcDataStore==null) schema._srcDataStore="";
		schema._srcDataField = tableElem.getAttribute("src-data-field");
		if (schema._srcDataField==null || schema._srcDataField.length() == 0) schema._srcDataField="src_data";
    schema._compressSrcData = true;
    String compress = tableElem.getAttribute("compress-src-data");
    if (compress != null && "false".equals(compress))
      schema._compressSrcData = false;
		
		NodeList columns = tableElem.getElementsByTagName("column");
		for (int i = 0; i < columns.getLength(); ++i) {
			try {
				Element column = (Element) columns.item(i);
				String n = column.getAttribute("name");
				String t = column.getAttribute("type");
				String frm = column.getAttribute("from");
				
				FieldDefinition fdef = new FieldDefinition();
				fdef.formatter = null;
				fdef.fromField = frm.length() > 0 ? frm : n;

				fdef.isMeta = true;
				
				fdef.isMulti = false;
				
				String isMultiString = column.getAttribute("multi");
				if (isMultiString!=null && isMultiString.trim().length()>0){
					fdef.isMulti = Boolean.parseBoolean(isMultiString);
				}
				
				String delimString = column.getAttribute("delimiter");
				if (delimString!=null && delimString.trim().length()>0){
					fdef.delim = delimString;
				}
				
				schema._fieldDefMap.put(n, fdef);
				
				if (t.equals("int")) {
					MetaType metaType = DefaultSenseiInterpreter.CLASS_METATYPE_MAP.get(int.class);
					String formatString = DefaultSenseiInterpreter.DEFAULT_FORMAT_STRING_MAP.get(metaType);
					fdef.formatter = new DecimalFormat(formatString, new DecimalFormatSymbols(Locale.US));
					fdef.type = int.class;
				} else if (t.equals("short")) {
					MetaType metaType = DefaultSenseiInterpreter.CLASS_METATYPE_MAP.get(short.class);
					String formatString = DefaultSenseiInterpreter.DEFAULT_FORMAT_STRING_MAP.get(metaType);
					fdef.formatter = new DecimalFormat(formatString, new DecimalFormatSymbols(Locale.US));
					fdef.type = int.class;
				} else if (t.equals("long")) {
					MetaType metaType = DefaultSenseiInterpreter.CLASS_METATYPE_MAP.get(long.class);
					String formatString = DefaultSenseiInterpreter.DEFAULT_FORMAT_STRING_MAP.get(metaType);
					fdef.formatter = new DecimalFormat(formatString, new DecimalFormatSymbols(Locale.US));
					fdef.type = long.class;
				} else if (t.equals("float")) {
					MetaType metaType = DefaultSenseiInterpreter.CLASS_METATYPE_MAP.get(float.class);
					String formatString = DefaultSenseiInterpreter.DEFAULT_FORMAT_STRING_MAP.get(metaType);
					fdef.formatter = new DecimalFormat(formatString, new DecimalFormatSymbols(Locale.US));
					fdef.type = double.class;
				} else if (t.equals("double")) {
					MetaType metaType = DefaultSenseiInterpreter.CLASS_METATYPE_MAP.get(double.class);
					String formatString = DefaultSenseiInterpreter.DEFAULT_FORMAT_STRING_MAP.get(metaType);
					fdef.formatter = new DecimalFormat(formatString, new DecimalFormatSymbols(Locale.US));
					fdef.type = double.class;
				} else if (t.equals("char")) {
					fdef.formatter = null;
				} else if (t.equals("string")) {
					fdef.formatter = null;
				} else if (t.equals("boolean")) {
					fdef.formatter = null;
				} else if (t.equals("date")) {

					String f = "";
					try {
						f = column.getAttribute("format");
					} catch (Exception ex) {
						logger.error(ex.getMessage(), ex);
					}
					if (f.isEmpty())
						throw new ConfigurationException("Date format cannot be empty.");

					fdef.formatter = new SimpleDateFormat(f);
					fdef.type = Date.class;
				}
				else if (t.equals("text")){
					fdef.isMeta = false;
					String idxString = column.getAttribute("index");
					String storeString = column.getAttribute("store");
					String tvString = column.getAttribute("termvector");
					Index idx = idxString == null ? Index.ANALYZED : DefaultSenseiInterpreter.INDEX_VAL_MAP.get(idxString.toUpperCase());
					Store store = storeString == null ? Store.NO : DefaultSenseiInterpreter.STORE_VAL_MAP.get(storeString.toUpperCase());
					TermVector tv = tvString == null ? TermVector.NO : DefaultSenseiInterpreter.TV_VAL_MAP.get(tvString.toUpperCase());
					
					if (idx==null || store==null || tv==null){
					  throw new ConfigurationException("Invalid indexing parameter specification");
					}
					
					IndexSpec indexingSpec = new IndexSpec();
					indexingSpec.store = store;
					indexingSpec.index = idx;
					indexingSpec.tv = tv;
					  
					fdef.textIndexSpec = indexingSpec; 
				}

			} catch (Exception e) {
				throw new ConfigurationException("Error parsing schema: "
						+ columns.item(i), e);
			}
		}
		return schema;
	}
}
