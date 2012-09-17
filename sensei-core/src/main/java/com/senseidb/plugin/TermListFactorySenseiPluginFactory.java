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
package com.senseidb.plugin;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.browseengine.bobo.facets.data.TermFixedLengthLongArrayListFactory;
import com.browseengine.bobo.facets.data.TermListFactory;

import com.senseidb.indexing.DefaultSenseiInterpreter;

public class TermListFactorySenseiPluginFactory implements SenseiPluginFactory<TermListFactory>
{
  public static final String FIXEDLENGTHLONG = "fixedlengthlong";
  public static final String LENGTH          = "length";

  public static final String TYPE    = "type";
  public static final String INT     = "int";
  public static final String STRING  = "string";
  public static final String SHORT   = "short";
  public static final String LONG    = "long";
  public static final String FLOAT   = "float";
  public static final String DOUBLE  = "double";
  public static final String CHAR    = "char";
  public static final String BOOLEAN = "boolean";
  public static final String DATE    = "date";

  private static final Map<String, Class> TYPE_CLASS_MAP = new HashMap<String, Class>();

  static
  {
    TYPE_CLASS_MAP.put(INT,     int.class);
    TYPE_CLASS_MAP.put(STRING,  String.class);
    TYPE_CLASS_MAP.put(SHORT,   short.class);
    TYPE_CLASS_MAP.put(LONG,    long.class);
    TYPE_CLASS_MAP.put(FLOAT,   float.class);
    TYPE_CLASS_MAP.put(DOUBLE,  double.class);
    TYPE_CLASS_MAP.put(CHAR,    char.class);
    TYPE_CLASS_MAP.put(BOOLEAN, boolean.class);
    TYPE_CLASS_MAP.put(DATE,    Date.class);
  }

  public static TermListFactory getFactory(String type)
  {
    Class cls = TYPE_CLASS_MAP.get(type);
    if (cls != null)
    {
      return DefaultSenseiInterpreter.getTermListFactory(cls);
    }

    return null;
  }

  @Override
  public TermListFactory getBean(Map<String,String> initProperties,
                                 String fullPrefix,
                                 SenseiPluginRegistry pluginRegistry)
  {
    String type = initProperties.get(TYPE);
    if (FIXEDLENGTHLONG.equals(type))
    {
      return new TermFixedLengthLongArrayListFactory(Integer.parseInt(initProperties.get(LENGTH)));
    }
    return getFactory(type);
  }
}
