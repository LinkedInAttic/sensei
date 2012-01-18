package com.senseidb.conf;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.configuration.FileConfiguration;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;

import com.senseidb.plugin.SenseiPluginFactory;
import com.senseidb.plugin.SenseiPluginRegistry;

public class SpringSenseiPluginFactory implements SenseiPluginFactory<List<?>>{
  private ApplicationContext context = null;  
  public final String SPRING_FILENAME = "springFile";
  public final String CLASS_TO_RETURN = "returnedClass";
  private Class<?> classToReturn;
  private List<Class> classes;
  @Override
  public synchronized List<?>  getBean(Map<String, String> initProperties, String fullPrefix, SenseiPluginRegistry pluginRegistry) {
    if (context == null) {
      if (!initProperties.containsKey(SPRING_FILENAME)) {
       throw new IllegalArgumentException("The configuration doesn't contain the property - " + SPRING_FILENAME);
      }
      if (!initProperties.containsKey(CLASS_TO_RETURN)) {
        throw new IllegalArgumentException("The configuration doesn't contain the property - " + CLASS_TO_RETURN);
       }
      String localName = initProperties.get(SPRING_FILENAME);
      String springFile = null;
      if (localName.contains("/") || localName.contains("\\")) {
        springFile = localName;
      } else {
        File directory = ((FileConfiguration) pluginRegistry.getConfiguration()).getFile().getParentFile();
        springFile = new File(directory, localName).getAbsolutePath();
      }
     
      
     
      try {        
       
          springFile = "file:" + springFile;
        
        context = new FileSystemXmlApplicationContext(springFile);
      
        classes = getClasses(initProperties.get(CLASS_TO_RETURN));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }   
    
    List<Object> ret = new ArrayList<Object>();
    for (Class classToReturn : classes) {
      for (String beanName : context.getBeanNamesForType(classToReturn)) {
        ret.add(context.getBean(beanName));
      }
    }
    return ret;
  }
  private List<Class> getClasses(String classesToReturn) throws ClassNotFoundException {
    List<String> classesStr = new ArrayList<String>();
    if (classesToReturn.contains(",")) {
      for (String cls : classesToReturn.split(",")) {
        classesStr.add(cls.trim());
      }
    } else {
      classesStr.add(classesToReturn.trim());
    }
    List<Class> ret = new ArrayList<Class>(classesStr.size());
    for (String cls : classesStr) {
      ret.add(Class.forName(cls));
    }
    return ret;
  }

}
