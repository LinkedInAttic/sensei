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

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.browseengine.bobo.facets.data.TermListFactory;
import com.browseengine.bobo.facets.FacetHandler;

public abstract class AbstractFacetHandlerSenseiPluginFactory implements SenseiPluginFactory<FacetHandler>
{
  public static final String DEPENDS = "depends";

  public Set<String> getDepends(Map<String, String> initProperties)
  {
    Set<String> depends = new HashSet<String>();

    String val = initProperties.get(DEPENDS);
    if (val != null)
    {
      for (String depend : val.split(","))
      {
        depend = depend.trim();
        if (depend.length() != 0)
          depends.add(depend);
      }
    }

    return depends;
  }

  @Override
  public abstract FacetHandler getBean(Map<String,String> initProperties,
                                       String fullPrefix,
                                       SenseiPluginRegistry pluginRegistry);
}
