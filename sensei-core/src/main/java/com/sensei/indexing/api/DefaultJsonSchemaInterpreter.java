package com.sensei.indexing.api;

import java.io.ByteArrayOutputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.Map.Entry;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.log4j.Logger;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.json.JSONException;
import org.json.JSONObject;

import proj.zoie.api.indexing.AbstractZoieIndexable;
import proj.zoie.api.indexing.AbstractZoieIndexableInterpreter;
import proj.zoie.api.indexing.ZoieIndexable;

import com.sensei.conf.SenseiSchema;
import com.sensei.conf.SenseiSchema.FieldDefinition;

public class DefaultJsonSchemaInterpreter extends
    AbstractZoieIndexableInterpreter<JSONObject> {

  private static final Logger logger = Logger.getLogger(DefaultJsonSchemaInterpreter.class);
  
  
  private final SenseiSchema _schema;
  private final Set<Entry<String,FieldDefinition>> entries;
  private final String _uidField;
  private final String _delField;
  private final String _skipField;
  private final String _srcDataStore;
  private final String _srcDataField;
  private final boolean _compressSrcData;
  
  private final Map<String,JsonValExtractor> _dateExtractorMap;
  
  private JsonFilter _jsonFilter = null;
  
  private CustomIndexingPipeline _customIndexingPipeline = null;
  
  public DefaultJsonSchemaInterpreter(SenseiSchema schema) throws ConfigurationException{
     _schema = schema;
     entries = _schema.getFieldDefMap().entrySet();
     _uidField = _schema.getUidField();
     _delField = _schema.getDeleteField();
     _skipField = _schema.getSkipField();
     _srcDataStore = _schema.getSrcDataStore();
     _srcDataField = _schema.getSrcDataField();
     _compressSrcData = _schema.isCompressSrcData();
     _dateExtractorMap = new HashMap<String,JsonValExtractor>();
     for (Entry<String,FieldDefinition> entry : entries){
       final FieldDefinition def = entry.getValue();
       if (Date.class.equals(def.type)){
         _dateExtractorMap.put(entry.getKey(), new JsonValExtractor(){

            @Override
            public Object extract(String val) {
              try{
                return ((SimpleDateFormat)(def.formatter)).parse(val);
              }
              catch(Exception e){
                throw new RuntimeException(e.getMessage(),e);
              }
            }
            
          });
       }
     }
  }
  
  private static interface JsonValExtractor{
    Object extract(String val);
  }
  
  private final static Map<Class,JsonValExtractor> ExtractorMap = new HashMap<Class,JsonValExtractor>();
  
  static{
    ExtractorMap.put(int.class, new JsonValExtractor(){

      @Override
      public Object extract(String val) {
        return (val == null || val.length()==0) ? 0 : Integer.parseInt(val);
      }
      
    });
    ExtractorMap.put(double.class, new JsonValExtractor(){

      @Override
      public Object extract(String val) {

        return (val == null || val.length()==0) ? 0 : Double.parseDouble(val);
      }
      
    });
    ExtractorMap.put(long.class, new JsonValExtractor(){

      @Override
      public Object extract(String val) {

        return (val == null || val.length()==0) ? 0 : Long.parseLong(val);
      }
      
    });
    ExtractorMap.put(String.class, new JsonValExtractor(){

      @Override
      public Object extract(String val) {
        return val;
      }
      
    });
    
  }
  
  public void setCustomIndexingPipeline(CustomIndexingPipeline customIndexingPipeline){
    _customIndexingPipeline = customIndexingPipeline;
  }
  
  public CustomIndexingPipeline getCustomIndexingPipeline(){
    return _customIndexingPipeline;
  }
  
  public void setJsonFilter(JsonFilter jsonFilter){
    _jsonFilter = jsonFilter;
  }
  
  @Override
  public ZoieIndexable convertAndInterpret(JSONObject obj) {
    final JSONObject src = obj;
    final JSONObject filtered;
    if (_jsonFilter!=null){
      try {
        filtered = _jsonFilter.filter(src);
      } catch (Exception e) {
        throw new RuntimeException(e.getMessage(),e);
      }
    }
    else {
      filtered = src;
    }
    return new AbstractZoieIndexable(){

      @Override
      public IndexingReq[] buildIndexingReqs() {

        org.apache.lucene.document.Document luceneDoc = new org.apache.lucene.document.Document(); 
        for (Entry<String,FieldDefinition> entry : entries){
          String name = entry.getKey();
          try{
            final FieldDefinition fldDef = entry.getValue();
            if (fldDef.isMeta){
            JsonValExtractor extractor = ExtractorMap.get(fldDef.type);
            if (extractor==null){
              if (Date.class.equals(fldDef.type)){
              extractor = _dateExtractorMap.get(name);
              }
              else{
                extractor = ExtractorMap.get(String.class);
              }
            }

            if (filtered.has(fldDef.fromField))
            {
              List<Object> vals = new LinkedList<Object>();
              if (filtered.isNull(fldDef.fromField)) continue;
              if (fldDef.isMulti){
                String val = filtered.optString(fldDef.fromField);

                if (val!=null && val.trim().length()>0){
                  StringTokenizer strtok = new StringTokenizer(val,fldDef.delim);
                  while(strtok.hasMoreTokens()){
                    String token = strtok.nextToken();
                    vals.add(extractor.extract(token));
                  }
                }
              }
              else{
                String val = filtered.optString(fldDef.fromField);
                if (val == null) continue;
                vals.add(extractor.extract(filtered.optString(fldDef.fromField)));
              }
                      
              for (Object val : vals){
                String strVal = null;
                if (fldDef.formatter!=null){
                  strVal = fldDef.formatter.format(val);
                }
                else{
                  strVal = String.valueOf(val);
                }
                Field metaField = new Field(name,strVal,Store.NO,Index.NOT_ANALYZED_NO_NORMS);
                metaField.setOmitTermFreqAndPositions(true);
                luceneDoc.add(metaField);
              }
            }
          }
          else{
            Field textField = new Field(name,filtered.optString(fldDef.fromField),
                fldDef.textIndexSpec.store,fldDef.textIndexSpec.index,fldDef.textIndexSpec.tv);
            luceneDoc.add(textField);
          }
          }
          catch(Exception e){
            logger.error("Problem extracting data for field: "+name,e);
            throw new RuntimeException(e);
          }
        }
        if (_srcDataStore != null) {
          if ("lucene".equals(_srcDataStore)) {
            String data;
            if (_srcDataField != null && _srcDataField.length() != 0 && src.has(_srcDataField))
              data = src.optString(_srcDataField);
            else
              data = src.toString();

            try{
              byte[] origBytes = data.getBytes("UTF-8");
              if (_compressSrcData) {
                ByteArrayOutputStream bout = new ByteArrayOutputStream();
                GZIPOutputStream gzipStream = new GZIPOutputStream(bout);

                gzipStream.write(origBytes);
                gzipStream.flush();
                gzipStream.close();
                bout.flush();

                byte[] compressedBytes = bout.toByteArray();
                Field storedData = new Field(SenseiSchema.SRC_DATA_COMPRESSED_FIELD_NAME, compressedBytes,Store.YES);
                luceneDoc.add(storedData);
              }
              else {
                Field storedData = new Field(SenseiSchema.SRC_DATA_FIELD_NAME, origBytes,Store.YES);
                luceneDoc.add(storedData);
              }
            }
            catch(Exception e)
            {
              logger.error("problem writing to store data: "+data,e);
            }
          }
        }
        
        if (_customIndexingPipeline != null){
          _customIndexingPipeline.applyCustomization(luceneDoc, _schema, filtered);
        }
        return new IndexingReq[]{new IndexingReq(luceneDoc)};
      }

      @Override
      public long getUID() {
        try {
          return filtered.getLong(_uidField);
        } catch (JSONException e) {
          throw new IllegalStateException(e.getMessage(),e);
        }
      }

      @Override
      public boolean isDeleted() {
        return filtered.optBoolean(_delField);
      }

      @Override
      public boolean isSkip() {
        return filtered.optBoolean(_skipField);
      }
      
    };
  }

}
