package com.senseidb.ba.plugins;

import java.io.File;

import proj.zoie.api.Zoie;
import proj.zoie.api.indexing.ZoieIndexableInterpreter;
import proj.zoie.impl.indexing.ZoieConfig;

import com.senseidb.conf.ZoieFactoryFactory;
import com.senseidb.search.node.SenseiIndexReaderDecorator;
import com.senseidb.search.node.SenseiZoieFactory;
@SuppressWarnings("unchecked")
public class ZeusFactoryFactory implements ZoieFactoryFactory {

 
  @Override
  public SenseiZoieFactory<?> getZoieFactory(final File idxDir, ZoieIndexableInterpreter<?> interpreter, final SenseiIndexReaderDecorator decorator,
      ZoieConfig config) {
    return new SenseiZoieFactory( idxDir, null, interpreter, decorator, config) {
      @Override
      public Zoie getZoieInstance(int nodeId, int partitionId) {
        return new ZeusIndexFactory(idxDir, new ZeusIndexReaderDecorator());
      }
      @Override
      public File getPath(int nodeId, int partitionId) {
        // TODO Auto-generated method stub
        return null;
      }};
  }
}
