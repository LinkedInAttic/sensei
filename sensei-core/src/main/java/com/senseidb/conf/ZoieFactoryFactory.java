package com.senseidb.conf;

import java.io.File;

import com.linkedin.zoie.api.indexing.ZoieIndexableInterpreter;
import com.linkedin.zoie.impl.indexing.ZoieConfig;

import com.senseidb.search.node.SenseiIndexReaderDecorator;
import com.senseidb.search.node.SenseiZoieFactory;

public interface ZoieFactoryFactory {
	SenseiZoieFactory<?> getZoieFactory(File idxDir,ZoieIndexableInterpreter<?> interpreter,SenseiIndexReaderDecorator decorator,ZoieConfig config);
}
