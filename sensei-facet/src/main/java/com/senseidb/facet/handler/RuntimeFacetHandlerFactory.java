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


/**
 * This interface is intended for using with RuntimeFacetHandler, which typically
 * have local data that make them not only NOT thread safe but also dependent on
 * request. So it is necessary to have different instance for different client or
 * request. Typically, the new instance need to be initialized before use.
 * @author xiaoyang
 *
 */
public interface RuntimeFacetHandlerFactory<P extends FacetHandlerInitializerParam, F extends RuntimeFacetHandler<?>>
{
  /**
   * @return the facet name of the RuntimeFacetHandler it creates.
   */
  String getName();

  /**
   * @return if this facet support empty params or not.
   */
  boolean initParamsRequired();

  /**
   * @param params the data used to initialize the RuntimeFacetHandler.
   * @return a new instance of 
   */
  F get(P params);
}
