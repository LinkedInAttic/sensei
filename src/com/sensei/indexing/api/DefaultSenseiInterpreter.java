package com.sensei.indexing.api;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.LinkedList;
import java.util.List;

import proj.zoie.api.indexing.AbstractZoieIndexableInterpreter;
import proj.zoie.api.indexing.ZoieIndexable;

import com.sensei.search.nodes.SenseiNode;

public class DefaultSenseiInterpreter<V> extends
		AbstractZoieIndexableInterpreter<V> {

	private Class<V> _cls;
	Field _uidField;
	List<Field> _metaFieldList;
	List<Field> _textFieldList;
	Method _deleteChecker;
	Method _skipChecker;
	
	public DefaultSenseiInterpreter(Class<V> cls){
	  _cls = cls;
	  _metaFieldList = new LinkedList<Field>();
	  _textFieldList = new LinkedList<Field>();
	  _uidField = null;
	  Field[] fields = cls.getFields();
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
	      _textFieldList.add(f);
	    }
	    else if (f.isAnnotationPresent(Meta.class)){
	      f.setAccessible(true);
	      _metaFieldList.add(f);
	    }
	  }
		
      if (_uidField == null){
        throw new IllegalStateException(cls + " does not have uid defined");
      }
	}
	
	@Override
	public ZoieIndexable convertAndInterpret(V obj) {
		// TODO Auto-generated method stub
		return null;
	}

	public static void main(String[] args) {
		DefaultSenseiInterpreter<SenseiNode> nodeInterpreter = new DefaultSenseiInterpreter(SenseiNode.class);
	}
	
	class TestObj{
		@Uid
		private long uid;
		
		@Text(name="text")
		private String content;
		
		@Meta
		private int age;
		
		@Meta(name="birthday",format="YYYYMMDD",type=MetaType.Date)
		private String bday;
		
		@Meta(type=MetaType.String)
		private short shortVal;
		
	}
}
