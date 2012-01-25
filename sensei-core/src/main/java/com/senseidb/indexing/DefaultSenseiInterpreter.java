package com.senseidb.indexing;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.Field.TermVector;

import proj.zoie.api.indexing.AbstractZoieIndexableInterpreter;
import proj.zoie.api.indexing.ZoieIndexable;

import com.browseengine.bobo.facets.data.PredefinedTermListFactory;
import com.browseengine.bobo.facets.data.TermListFactory;

public class DefaultSenseiInterpreter<V> extends
		AbstractZoieIndexableInterpreter<V> {

	private static Logger logger = Logger.getLogger(DefaultSenseiInterpreter.class);
	

	public static final Map<MetaType,String> DEFAULT_FORMAT_STRING_MAP = new HashMap<MetaType,String>();
	public static final Map<Class,MetaType>  CLASS_METATYPE_MAP = new HashMap<Class,MetaType>();
	
	public static final Map<String,Index>  INDEX_VAL_MAP = new HashMap<String,Index>();
	public static final Map<String,Store>  STORE_VAL_MAP = new HashMap<String,Store>();
	public static final Map<String,TermVector>  TV_VAL_MAP = new HashMap<String,TermVector>();		
	
	static{
		DEFAULT_FORMAT_STRING_MAP.put(MetaType.Integer, "00000000000000000000");
		DEFAULT_FORMAT_STRING_MAP.put(MetaType.Short, "00000");
		DEFAULT_FORMAT_STRING_MAP.put(MetaType.Long, "0000000000000000000000000000000000000000");
		DEFAULT_FORMAT_STRING_MAP.put(MetaType.Date, "yyyyMMdd-kk:mm");
		DEFAULT_FORMAT_STRING_MAP.put(MetaType.Float, "00000000000000000000.00");
		DEFAULT_FORMAT_STRING_MAP.put(MetaType.Double, "0000000000000000000000000000000000000000.00");
		
		CLASS_METATYPE_MAP.put(String.class, MetaType.String);
		CLASS_METATYPE_MAP.put(int.class, MetaType.Integer);
		CLASS_METATYPE_MAP.put(Integer.class, MetaType.Integer);
		CLASS_METATYPE_MAP.put(short.class, MetaType.Short);
		CLASS_METATYPE_MAP.put(Short.class, MetaType.Short);
		CLASS_METATYPE_MAP.put(long.class, MetaType.Long);
		CLASS_METATYPE_MAP.put(Long.class, MetaType.Long);
		CLASS_METATYPE_MAP.put(float.class, MetaType.Float);
		CLASS_METATYPE_MAP.put(Float.class, MetaType.Float);
		CLASS_METATYPE_MAP.put(double.class, MetaType.Double);
		CLASS_METATYPE_MAP.put(Double.class, MetaType.Double);
		CLASS_METATYPE_MAP.put(char.class, MetaType.Char);
		CLASS_METATYPE_MAP.put(Character.class, MetaType.Char);
		CLASS_METATYPE_MAP.put(boolean.class, MetaType.Boolean);
		CLASS_METATYPE_MAP.put(Boolean.class, MetaType.Boolean);
		CLASS_METATYPE_MAP.put(Date.class, MetaType.Date);
		
		INDEX_VAL_MAP.put("NO",Index.NO);
		INDEX_VAL_MAP.put("ANALYZED",Index.ANALYZED);
		INDEX_VAL_MAP.put("TOKENIZED",Index.ANALYZED);
		INDEX_VAL_MAP.put("NOT_ANALYZED",Index.NOT_ANALYZED);
		INDEX_VAL_MAP.put("UN_TOKENIZED",Index.NOT_ANALYZED);
		INDEX_VAL_MAP.put("NOT_ANALYZED_NO_NORMS",Index.NOT_ANALYZED_NO_NORMS);
		INDEX_VAL_MAP.put("NO_NORMS",Index.NOT_ANALYZED_NO_NORMS);
		INDEX_VAL_MAP.put("NOT_ANALYZED_NO_NORMS",Index.NOT_ANALYZED_NO_NORMS);
		INDEX_VAL_MAP.put("ANALYZED_NO_NORMS",Index.ANALYZED_NO_NORMS);
		
		STORE_VAL_MAP.put("NO",Store.NO);
		STORE_VAL_MAP.put("YES",Store.YES);
		
		TV_VAL_MAP.put("NO",TermVector.NO);
		TV_VAL_MAP.put("YES",TermVector.YES);
		TV_VAL_MAP.put("WITH_POSITIONS",TermVector.WITH_POSITIONS);
		TV_VAL_MAP.put("WITH_OFFSETS",TermVector.WITH_OFFSETS);
		TV_VAL_MAP.put("WITH_POSITIONS_OFFSETS",TermVector.WITH_POSITIONS_OFFSETS);
	}
	
	public static <T> TermListFactory<T> getTermListFactory(Class<T> cls){
		MetaType metaType = CLASS_METATYPE_MAP.get(cls);
		if (metaType == null){
			throw new IllegalArgumentException("unsupported class: "+cls.getName());
		}
		return new PredefinedTermListFactory<T>(cls,DEFAULT_FORMAT_STRING_MAP.get(metaType));
	}
	
	
	public static class IndexSpec{
	  public Store store;
	  public Index index;
	  public TermVector tv;
	  Field fld;
	}
	
	static class MetaFormatSpec{
	  Format formatter;
	  Field fld;
	}
	
	private Class<V> _cls;

	final Map<String,IndexSpec> _textIndexingSpecMap;
	final Map<String,MetaFormatSpec> _metaFormatSpecMap;
	
	Field _uidField;
	Method _deleteChecker;
	Method _skipChecker;
	
	public DefaultSenseiInterpreter(Class<V> cls){
	  _cls = cls;
	  _metaFormatSpecMap = new HashMap<String,MetaFormatSpec>();
	  _textIndexingSpecMap = new HashMap<String,IndexSpec>();
	  _uidField = null;
	  Field[] fields = cls.getDeclaredFields();
	  for (Field f : fields){
	    if (f.isAnnotationPresent(Uid.class)){
		  if (_uidField != null){
		    throw new IllegalStateException("multiple uids defined in class: "+cls);
		  }
		  else{
			Class fieldType = f.getType();
			if (fieldType.isPrimitive()){
			  if (int.class.equals(fieldType) || short.class.equals(fieldType) || long.class.equals(fieldType)){
				  _uidField = f;
				  _uidField.setAccessible(true);
			  }
			}
			if (_uidField == null){
				throw new IllegalStateException("uid field's type must be one of long, int or short");
			}
		  }
	    }
	    else if (f.isAnnotationPresent(Text.class)){
	      f.setAccessible(true);
	      Text textAnnotation = f.getAnnotation(Text.class);
	      String name=textAnnotation.name();
		  if ("".equals(name)){
			name = f.getName();
		  }
		  
		  Index idx = INDEX_VAL_MAP.get(textAnnotation.index());
		  Store store = STORE_VAL_MAP.get(textAnnotation.store());
		  TermVector tv = TV_VAL_MAP.get(textAnnotation.termVector());
		
		  if (idx==null || store==null || tv==null){
			throw new RuntimeException("Invalid indexing parameter specification");
		  }
		  IndexSpec indexingSpec = new IndexSpec();
		  indexingSpec.store = store;
		  indexingSpec.index = idx;
		  indexingSpec.tv = tv;
		  indexingSpec.fld = f;
		  _textIndexingSpecMap.put(name, indexingSpec);
	    }
	    else if (f.isAnnotationPresent(StoredValue.class)){
		      f.setAccessible(true);
		      StoredValue storeAnnotation = f.getAnnotation(StoredValue.class);
		      String name=storeAnnotation.name();
			  if ("".equals(name)){
				name = f.getName();
			  }
			  IndexSpec indexingSpec = new IndexSpec();
			  indexingSpec.store = Store.YES;
			  indexingSpec.index = Index.NO;
			  indexingSpec.tv = TermVector.NO;
			  indexingSpec.fld = f;
			  _textIndexingSpecMap.put(name, indexingSpec);
		}
	    else if (f.isAnnotationPresent(Meta.class)){
	      f.setAccessible(true);
	      Meta metaAnnotation = f.getAnnotation(Meta.class);
		    String name=metaAnnotation.name();
		    if ("".equals(name)){
			  name = f.getName();
		    }
		    MetaType metaType = metaAnnotation.type();
		    if (MetaType.Auto.equals(metaType)){
		    	Class typeClass = f.getType();
		    	if (Collection.class.isAssignableFrom(typeClass)){
		    		metaType = MetaType.String;
		    	}
		    	else{
		    	  metaType = CLASS_METATYPE_MAP.get(typeClass);
		    	  if (metaType==null){
		    		metaType = MetaType.String;
		    	  }
		    	}
		    }
		    String defaultFormatString = DEFAULT_FORMAT_STRING_MAP.get(metaType);
		    String formatString = metaAnnotation.format();
		    if ("".equals(formatString)){
		    	formatString = defaultFormatString;
		    }
		    
		    MetaFormatSpec formatSpec = new MetaFormatSpec();
	    	_metaFormatSpecMap.put(name, formatSpec);
		    formatSpec.fld = f;
		    if (defaultFormatString!=null){
		      if (MetaType.Date == metaType){
		    	formatSpec.formatter = new SimpleDateFormat(formatString);
		      }
		      else{
		    	formatSpec.formatter = new DecimalFormat(formatString, new DecimalFormatSymbols(Locale.US));
		      }
		    }
	    }
	  }
	  
	  Method[] methods = cls.getDeclaredMethods();
	  for (Method method : methods){
		  if (method.isAnnotationPresent(DeleteChecker.class)){
			if (_deleteChecker==null){
				method.setAccessible(true);
				_deleteChecker = method;
			}
			else{
				throw new IllegalStateException("more than 1 delete checker defined in class: "+cls);
			}
		  }
		  else if (method.isAnnotationPresent(SkipChecker.class)){
			if (_skipChecker==null){
				method.setAccessible(true);
				_skipChecker = method;
			}
			else{
				throw new IllegalStateException("more than 1 skip checker defined in class: "+cls);
			}  
		  }
	  }
		
      if (_uidField == null){
        throw new IllegalStateException(cls + " does not have uid defined");
      }
	}
	
	@Override
	public String toString(){
		StringBuilder buf = new StringBuilder();
		buf.append("data class name: ").append(_cls.getName());
		buf.append("\n_uid field: ").append(_uidField.getName());
		buf.append("\ndelete checker: ").append(_deleteChecker==null?"none":_deleteChecker.getName());
		buf.append("\nskip checker: ").append(_skipChecker==null?"none":_skipChecker.getName());
		buf.append("\ntext fields: ");
		if (_textIndexingSpecMap.size()==0){
			buf.append("none");
		}
		else{
			boolean first = true;
			Set<String> tfNames = _textIndexingSpecMap.keySet();
			for (String name : tfNames){
				if (!first){
					buf.append(",");
				}
				else{
					first=false;
				}
				buf.append(name);
			}
		}
		buf.append("\nmeta fields: ");
		if (_metaFormatSpecMap.size()==0){
			buf.append("none");
		}
		else{
			boolean first = true;
			Set<String> tfNames = _metaFormatSpecMap.keySet();
			for (String tf : tfNames){
				if (!first){
					buf.append(",");
				}
				else{
					first=false;
				}
				buf.append(tf);
			}
		}
		return buf.toString();
	}
	
	@Override
	public ZoieIndexable convertAndInterpret(V obj) {
		return new DefaultSenseiZoieIndexable<V>(obj,this);
	}
	
}
