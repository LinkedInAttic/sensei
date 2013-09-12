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

package com.senseidb.facet.handler;


import com.senseidb.facet.search.FacetAtomicReader;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;


/**
 * Abstract class for RuntimeFacetHandlers. A concrete RuntimeFacetHandler should implement
 * the FacetHandlerFactory and RuntimeInitializable so that bobo knows how to create new
 * instance of the handler at run time and how to initialize it at run time respectively.
 * @author ymatsuda
 * @param <D> type parameter for FacetData
 */
public abstract class RuntimeFacetHandler<D> extends LoadableFacetHandler<D>
{
  Map<FacetAtomicReader, Object> _runtimeData;

  /**
   * Constructor that specifying the dependent _facet handlers using names.
   * @param name the name of this FacetHandler, which is used in FacetSpec and Selection to specify
   * the _facet. If we regard a _facet as a field, the name is like a field name.
   * @param dependsOn Set of names of _facet handlers this _facet handler depend on for loading.
   */
  public RuntimeFacetHandler(String name, Set<String> dependsOn)
  {
    super(name, dependsOn);
    _runtimeData = new HashMap<FacetAtomicReader, Object>();
  }
  
  /**
   * Constructor
   * @param name the name of this FacetHandler, which is used in FacetSpec and Selection to specify
   * the _facet. If we regard a _facet as a field, the name is like a field name.
   */
  public RuntimeFacetHandler(String name)
  {
      super(name);
  }
  
  
  @Override
  @SuppressWarnings("unchecked")
  public D getFacetData(FacetAtomicReader reader)
  {
    return (D)_runtimeData.get(reader);
  }

  public void putFacetData(FacetAtomicReader reader, Object data)
  {
    _runtimeData.put(reader, data);
  }

  public void close()
  {
  }
}
