package com.sensei.indexing.api;

import java.text.DecimalFormat;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.Map.Entry;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.log4j.Logger;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.Field.TermVector;
import org.json.JSONException;
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
		boolean isMulti;
		String delim = ",";
	}
	
	private final Map<String,FieldDefinition> _fieldDefMap;
	private final String _uidField;
	private String _deleteField;
	private String _skipField;
	
	public DefaultJsonSchemaInterpreter(org.w3c.dom.Document schemaDoc) throws ConfigurationException{
		_fieldDefMap = new HashMap<String,FieldDefinition>();
		NodeList tables = schemaDoc.getElementsByTagName("table");
		if (tables==null || tables.getLength()==0){
			throw new ConfigurationException("empty schema");
		}
		if (tables.getLength()>1){
			throw new ConfigurationException("multiple schemas not supported");
		}
		
		Element tableElem = (Element) tables.item(0);
		_uidField = tableElem.getAttribute("uid");
		_deleteField = tableElem.getAttribute("delete-field");
		if (_deleteField==null) _deleteField="";
		_skipField = tableElem.getAttribute("skip-field");
		if (_skipField==null) _skipField="";
		
		NodeList columns = tableElem.getElementsByTagName("column");
		for (int i = 0; i < columns.getLength(); ++i) {
			try {
				Element column = (Element) columns.item(i);
				String n = column.getAttribute("name");
				String t = column.getAttribute("type");
				String frm = column.getAttribute("from");
				
				FieldDefinition fdef = new FieldDefinition();
				fdef.formatter = null;
				fdef.fromField = frm == null ? frm : n;
				fdef.isMeta = true;
				
				fdef.isMulti = false;
				
				String isMultiString = column.getAttribute("multi");
				if (isMultiString!=null && isMultiString.trim().length()>0){
					fdef.isMulti = Boolean.parseBoolean(isMultiString);
				}
				
				String delimString = column.getAttribute("multi");
				if (delimString!=null && delimString.trim().length()>0){
					fdef.delim = delimString;
				}
				
				_fieldDefMap.put(n, fdef);
				
				if (t.equals("int")) {
					MetaType metaType = DefaultSenseiInterpreter.CLASS_METATYPE_MAP.get(int.class);
					String formatString = DefaultSenseiInterpreter.DEFAULT_FORMAT_STRING_MAP.get(metaType);
					fdef.formatter = new DecimalFormat(formatString);
				} else if (t.equals("short")) {
					MetaType metaType = DefaultSenseiInterpreter.CLASS_METATYPE_MAP.get(short.class);
					String formatString = DefaultSenseiInterpreter.DEFAULT_FORMAT_STRING_MAP.get(metaType);
					fdef.formatter = new DecimalFormat(formatString);
				} else if (t.equals("long")) {
					MetaType metaType = DefaultSenseiInterpreter.CLASS_METATYPE_MAP.get(long.class);
					String formatString = DefaultSenseiInterpreter.DEFAULT_FORMAT_STRING_MAP.get(metaType);
					fdef.formatter = new DecimalFormat(formatString);
				} else if (t.equals("float")) {
					MetaType metaType = DefaultSenseiInterpreter.CLASS_METATYPE_MAP.get(float.class);
					String formatString = DefaultSenseiInterpreter.DEFAULT_FORMAT_STRING_MAP.get(metaType);
					fdef.formatter = new DecimalFormat(formatString);
				} else if (t.equals("double")) {
					MetaType metaType = DefaultSenseiInterpreter.CLASS_METATYPE_MAP.get(double.class);
					String formatString = DefaultSenseiInterpreter.DEFAULT_FORMAT_STRING_MAP.get(metaType);
					fdef.formatter = new DecimalFormat(formatString);
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
	}
	
	@Override
	public ZoieIndexable convertAndInterpret(final JSONObject src) {
		return new ZoieIndexable(){

			@Override
			public IndexingReq[] buildIndexingReqs() {
				Set<Entry<String,FieldDefinition>> entries = _fieldDefMap.entrySet();
				org.apache.lucene.document.Document luceneDoc = new org.apache.lucene.document.Document(); 
				for (Entry<String,FieldDefinition> entry : entries){
				  String name = entry.getKey();
				  FieldDefinition fldDef = entry.getValue();
				  
				  if (fldDef.isMeta){
					  List<String> vals = new LinkedList<String>();
					  if (fldDef.isMulti){
						String val = src.optString(fldDef.fromField);
						
						if (val!=null && val.trim().length()>0){
							StringTokenizer strtok = new StringTokenizer(fldDef.delim);
							while(strtok.hasMoreTokens()){
								vals.add(strtok.nextToken());
							}
						}
					  }
					  else{
					    vals.add(src.optString(fldDef.fromField));
					  }
					  for (String val : vals){
						if (fldDef.formatter!=null){
							val = fldDef.formatter.format(val);
						}
						Field metaField = new Field(name,val,Store.NO,Index.NOT_ANALYZED_NO_NORMS);
						metaField.setOmitTermFreqAndPositions(true);
						luceneDoc.add(metaField);
					  }
				  }
				  else{
					  Field textField = new Field(name,src.optString(fldDef.fromField),
							  fldDef.textIndexSpec.store,fldDef.textIndexSpec.index,fldDef.textIndexSpec.tv);
					  luceneDoc.add(textField);
				  }
				}
				return new IndexingReq[]{new IndexingReq(luceneDoc)};
			}

			@Override
			public long getUID() {
				try {
					return src.getLong(_uidField);
				} catch (JSONException e) {
					throw new IllegalStateException(e.getMessage(),e);
				}
			}

			@Override
			public boolean isDeleted() {
				return src.optBoolean(_deleteField);
			}

			@Override
			public boolean isSkip() {
				return src.optBoolean(_skipField);
			}
			
		};
	}

}
