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
      
        classToReturn = Class.forName(initProperties.get(CLASS_TO_RETURN));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }   
    
    List<Object> ret = new ArrayList<Object>();
    for (String beanName : context.getBeanNamesForType(classToReturn)) {
      ret.add(context.getBean(beanName));
    }
    return ret;
  }

}
