package com.senseidb.search.node.impl;

import java.io.File;

import com.linkedin.zoie.api.DirectoryManager.DIRECTORY_MODE;
import com.linkedin.zoie.api.indexing.ZoieIndexableInterpreter;
import com.linkedin.zoie.impl.indexing.ZoieConfig;
import com.linkedin.zoie.impl.indexing.ZoieSystem;

import com.linkedin.bobo.api.BoboIndexReader;
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
