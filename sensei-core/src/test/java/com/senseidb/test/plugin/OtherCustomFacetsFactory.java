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

package com.senseidb.test.plugin;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import com.browseengine.bobo.facets.FacetHandler;
import com.browseengine.bobo.facets.data.PredefinedTermListFactory;
import com.browseengine.bobo.facets.impl.SimpleFacetHandler;
import com.senseidb.plugin.SenseiPluginFactory;
import com.senseidb.plugin.SenseiPluginRegistry;

public class OtherCustomFacetsFactory implements SenseiPluginFactory<List<FacetHandler<?>>>{

  @Override
  public List<FacetHandler<?>> getBean(Map<String, String> initProperties, String fullPrefix,
      SenseiPluginRegistry pluginRegistry) {
    List<FacetHandler<?>> ret = new ArrayList<FacetHandler<?>>();
    ret.add(new SimpleFacetHandler("handler1", "field1" , new PredefinedTermListFactory(Long.class), new HashSet<String>()));
    ret.add(new SimpleFacetHandler("handler2", "field2" , new PredefinedTermListFactory(Long.class), new HashSet<String>()));
    ret.add(new SimpleFacetHandler("handler3", "field3" , new PredefinedTermListFactory(Long.class), new HashSet<String>()));
    return ret;
  }

}
