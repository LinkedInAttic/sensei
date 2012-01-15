package com.senseidb.conf;

import java.io.File;

import proj.zoie.api.indexing.ZoieIndexableInterpreter;
import proj.zoie.impl.indexing.ZoieConfig;

import com.senseidb.search.node.SenseiIndexReaderDecorator;
import com.senseidb.search.node.SenseiZoieFactory;

public interface ZoieFactoryFactory {
	SenseiZoieFactory<?> getZoieFactory(File idxDir,ZoieIndexableInterpreter<?> interpreter,SenseiIndexReaderDecorator decorator,ZoieConfig config);
}
