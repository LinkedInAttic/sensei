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

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Set;


/**
 * The dummy interface to indicate that a class type can be used for initializing RuntimeFacetHandlers.
 * @author xiaoyang
 *
 */
public abstract class FacetHandlerInitializerParam implements Serializable
{
  private static final long serialVersionUID = 1L;

  public static final FacetHandlerInitializerParam EMPTY_PARAM = new FacetHandlerInitializerParam()
  {
    @Override
    public List<String> getStringParam(String name)
    {
      return null;
    }

    @Override
    public int[] getIntParam(String name)
    {
      return null;
    }

    @Override
    public boolean[] getBooleanParam(String name)
    {
      return null;
    }

    @Override
    public long[] getLongParam(String name)
    {
      return null;
    }

    @Override
    public byte[] getByteArrayParam(String name)
    {
      return null;
    }

    @Override
    public double[] getDoubleParam(String name)
    {
      return null;
    }

    @Override
    public Set<String> getBooleanParamNames()
    {
      return Collections.EMPTY_SET;
    }

    @Override
    public Set<String> getStringParamNames()
    {
      return Collections.EMPTY_SET;
    }

    @Override
    public Set<String> getIntParamNames()
    {
      return Collections.EMPTY_SET;
    }

    @Override
    public Set<String> getByteArrayParamNames()
    {
      return Collections.EMPTY_SET;
    }

    @Override
    public Set<String> getLongParamNames()
    {
      return Collections.EMPTY_SET;
    }

    @Override
    public Set<String> getDoubleParamNames()
    {
      return Collections.EMPTY_SET;
    }
  };

  public abstract List<String> getStringParam(String name);
  public abstract int[] getIntParam(String name);
  public abstract boolean[] getBooleanParam(String name);
  public abstract long[] getLongParam(String name);
  public abstract byte[] getByteArrayParam(String name);
  public abstract double[] getDoubleParam(String name);
  public abstract Set<String> getBooleanParamNames();
  public abstract Set<String> getStringParamNames();
  public abstract Set<String> getIntParamNames();
  public abstract Set<String> getByteArrayParamNames();
  public abstract Set<String> getLongParamNames();
  public abstract Set<String> getDoubleParamNames();
}
