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
package com.senseidb.search.node.impl;

import java.io.File;

import proj.zoie.api.DirectoryManager.DIRECTORY_MODE;
import proj.zoie.api.indexing.ZoieIndexableInterpreter;
import proj.zoie.impl.indexing.ZoieConfig;
import proj.zoie.impl.indexing.ZoieSystem;

import com.browseengine.bobo.api.BoboIndexReader;
import com.senseidb.conf.ZoieFactoryFactory;
import com.senseidb.search.node.SenseiIndexReaderDecorator;
import com.senseidb.search.node.SenseiZoieFactory;
import com.senseidb.search.node.SenseiZoieSystemFactory;

public class DemoZoieSystemFactory<T> extends SenseiZoieSystemFactory<T>
{
  private ZoieSystem<BoboIndexReader,T> _zoieSystem = null;
  
  public DemoZoieSystemFactory(File idxDir, ZoieIndexableInterpreter<T> interpreter, SenseiIndexReaderDecorator indexReaderDecorator,
                               ZoieConfig zoieConfig)
  {
    super(idxDir, DIRECTORY_MODE.SIMPLE, interpreter, indexReaderDecorator, zoieConfig);
  }
  
  public DemoZoieSystemFactory(File idxDir,ZoieIndexableInterpreter<T> interpreter,ZoieConfig zoieConfig){
    super(idxDir, DIRECTORY_MODE.SIMPLE, interpreter, new SenseiIndexReaderDecorator(), zoieConfig);
  }
  
  @Override
  public ZoieSystem<BoboIndexReader,T> getZoieInstance(int nodeId,int partitionId)
  {
    if(_zoieSystem == null)
    {
      _zoieSystem = super.getZoieInstance(nodeId,partitionId);
    }
    return _zoieSystem;
  }
  
  @Override
  public File getPath(int nodeId,int partitionId)
  {
    return _idxDir;
  }
  
  public static class DemoZoieFactoryFactory implements ZoieFactoryFactory{

	@Override
	public SenseiZoieFactory<?> getZoieFactory(File idxDir,
			ZoieIndexableInterpreter<?> interpreter,
			SenseiIndexReaderDecorator decorator, ZoieConfig config) {
		return new DemoZoieSystemFactory(idxDir,interpreter,decorator,config);
	}
  }
}
