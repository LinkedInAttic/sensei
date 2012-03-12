package com.senseidb.indexing;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.log4j.Logger;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.json.JSONException;
import org.json.JSONObject;

import proj.zoie.api.indexing.AbstractZoieIndexable;
import proj.zoie.api.indexing.AbstractZoieIndexableInterpreter;
import proj.zoie.api.indexing.ZoieIndexable;

import com.senseidb.conf.SenseiSchema;
import com.senseidb.conf.SenseiSchema.FieldDefinition;

public class DefaultJsonSchemaInterpreter extends
    AbstractZoieIndexableInterpreter<JSONObject> {

  private static final Logger logger = Logger.getLogger(DefaultJsonSchemaInterpreter.class);
  
  
  private final SenseiSchema _schema;
  private final Set<Entry<String,FieldDefinition>> entries;
  private final String _uidField;
  private final String _delField;
  private final String _skipField;
  private final boolean _compressSrcData;
  
  private final Map<String,JsonValExtractor> _dateExtractorMap;
  
  private JsonFilter _jsonFilter = null;
  
  private static Charset UTF8 = Charset.forName("UTF-8");
  
  private CustomIndexingPipeline _customIndexingPipeline = null;
  
  public DefaultJsonSchemaInterpreter(SenseiSchema schema) throws ConfigurationException{
     _schema = schema;
     entries = _schema.getFieldDefMap().entrySet();
     _uidField = _schema.getUidField();
     _delField = _schema.getDeleteField();
     _skipField = _schema.getSkipField();
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
        if (val == null || val.length()==0){
          return 0;
        }
        else{
          int num = Integer.parseInt(val);
          /*if (num<0){
            logger.error("we don't yet support negative values, skipping.");
            return null;
          }*/
          return num;
        }
      }
      
    });
    ExtractorMap.put(double.class, new JsonValExtractor(){

      @Override
      public Object extract(String val) {

        if (val == null || val.length()==0){
          return 0.0;
        }
        else{
          double num = Double.parseDouble(val);
          /*if (num<0.0){
            logger.error("we don't yet support negative values, skipping.");
            return null;
          }*/
          return num;
        }
      }
      
    });
    ExtractorMap.put(long.class, new JsonValExtractor(){

      @Override
      public Object extract(String val) {
        if (val == null || val.length()==0){
          return 0.0;
        }
        else{
          long num = Long.parseLong(val);
         /* if (num<0){
            logger.error("we don't yet support negative values, skipping.");
            return null;
          }*/
          return num;
        }
      }
      
    });
    ExtractorMap.put(String.class, new JsonValExtractor(){

      @Override
      public Object extract(String val) {
        return val;
      }
      
    });
    
  }

  public static byte[] compress(byte[] src) throws Exception
  {
    byte[] data = null;
    if (src != null)
    {
      ByteArrayOutputStream bout = new ByteArrayOutputStream();
      GZIPOutputStream gzipStream = new GZIPOutputStream(bout);

      gzipStream.write(src);
      gzipStream.flush();
      gzipStream.close();
      bout.flush();

      data = bout.toByteArray();
    }

    return data;
  }

  public static byte[] decompress(byte[] src) throws Exception
  {
    byte[] data = null;
    if (src != null)
    {
      ByteArrayOutputStream bout = new ByteArrayOutputStream();
      byte[] buf = new byte[1024];  // 1k buffer
      ByteArrayInputStream bin = new ByteArrayInputStream(src);
      GZIPInputStream gzipStream = new GZIPInputStream(bin);

      int len;
      while ((len = gzipStream.read(buf)) > 0) {
        bout.write(buf, 0, len);
      }
      bout.flush();

      data = bout.toByteArray();
    }

    return data;
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
                    Object obj = extractor.extract(token);
                    if (obj!=null){
                      vals.add(obj);
                    }
                  }
                }
              }
              else{
                String val = filtered.optString(fldDef.fromField);
                if (val == null) continue;
                Object obj = extractor.extract(filtered.optString(fldDef.fromField));
                if (obj!=null){
                  vals.add(obj);
                }
              }
                      
              for (Object val : vals){
                if (val==null) continue;
                String strVal = null;
                if (fldDef.formatter!=null){
                  strVal = fldDef.formatter.format(val);
                }
                else{
                  strVal = String.valueOf(val);
                }
                Field metaField = new Field(name,strVal,Store.NO,Index.NOT_ANALYZED_NO_NORMS);
                metaField.setOmitNorms(true);
                metaField.setIndexOptions(IndexOptions.DOCS_ONLY);
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
        
        if (_customIndexingPipeline != null){
          _customIndexingPipeline.applyCustomization(luceneDoc, _schema, filtered);
        }
        return new IndexingReq[]{new IndexingReq(luceneDoc)};
      }

      @Override
      public long getUID() {
        try {
          return Long.parseLong(filtered.getString(_uidField));
        } catch (JSONException e) {
          throw new IllegalStateException(e.getMessage(),e);
        }
      }

      @Override
      public boolean isDeleted()
      {
        try
        {
          String type = filtered.optString(SenseiSchema.EVENT_TYPE_FIELD, null);
          if (type == null)
            return filtered.optBoolean(_delField);
          else
            return SenseiSchema.EVENT_TYPE_DELETE.equalsIgnoreCase(type);
        }
        catch(Exception e)
        {
          logger.error(e.getMessage(), e);
          return false;
        }
      }

      @Override
      public boolean isSkip()
      {
        try
        {
          String type = filtered.optString(SenseiSchema.EVENT_TYPE_FIELD, null);
          if (type == null)
            return filtered.optBoolean(_skipField);
          else
            return SenseiSchema.EVENT_TYPE_SKIP.equalsIgnoreCase(type);
        }
        catch(Exception e)
        {
          logger.error(e.getMessage(), e);
          return false;
        }
      }

      @Override
      public byte[] getStoreValue()
      {
        byte[] data = null;
        if (src != null)
        {
          Object type = src.remove(SenseiSchema.EVENT_TYPE_FIELD);
          try
          {
            if (_compressSrcData)
              data = compress(src.toString().getBytes("UTF-8"));
            else
              data = src.toString().getBytes("UTF-8");
          }
          catch (Exception e)
          {
            logger.error(e.getMessage(), e);
          }

          if (type != null)
          {
            try
            {
              src.put(SenseiSchema.EVENT_TYPE_FIELD, type);
            }
            catch(Exception e)
            {
              logger.error("Should never happen", e);
            }
          }
        }
        
        return data;
      }

      @Override
      public boolean isStorable() {
        return true;
      }
      
      
      
    };
  }

}
