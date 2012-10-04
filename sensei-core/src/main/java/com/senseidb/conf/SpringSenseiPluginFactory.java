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
