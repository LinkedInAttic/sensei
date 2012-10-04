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
package com.senseidb.search.node;

import java.io.File;
import java.util.Comparator;

import proj.zoie.api.Zoie;
import proj.zoie.api.DirectoryManager.DIRECTORY_MODE;
import proj.zoie.api.indexing.ZoieIndexableInterpreter;
import proj.zoie.impl.indexing.ZoieConfig;

import com.browseengine.bobo.api.BoboIndexReader;

public abstract class SenseiZoieFactory<D>
{ 
	
  protected final File _idxDir;
  protected final ZoieIndexableInterpreter<D> _interpreter;
  protected final SenseiIndexReaderDecorator _indexReaderDecorator;
  protected final ZoieConfig _zoieConfig;
  protected final DIRECTORY_MODE _dirMode; 
  
  public SenseiZoieFactory(File idxDir,DIRECTORY_MODE dirMode,ZoieIndexableInterpreter<D> interpreter,SenseiIndexReaderDecorator indexReaderDecorator,ZoieConfig zoieConfig){
	  _idxDir = idxDir;
	  _interpreter = interpreter;
	  _indexReaderDecorator = indexReaderDecorator;
	  _zoieConfig = zoieConfig;
	  _dirMode = dirMode;
  }
  
  public static File getPath(File idxDir,int nodeId,int partitionId){
    File nodeLevelFile = new File(idxDir, "node"+nodeId);  
    return new File(nodeLevelFile, "shard"+partitionId); 
  }
  
  public SenseiIndexReaderDecorator getDecorator(){
	  return _indexReaderDecorator;
  }
  
  public ZoieIndexableInterpreter<D> getInterpreter(){
	  return _interpreter;
  }

  public Comparator<String> getVersionComparator() {
    return _zoieConfig.getVersionComparator();
  }
  
  public abstract Zoie<BoboIndexReader,D> getZoieInstance(int nodeId,int partitionId);

  // TODO: change to getDirectoryManager
  public abstract File getPath(int nodeId,int partitionId);
}
