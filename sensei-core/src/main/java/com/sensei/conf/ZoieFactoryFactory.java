package com.sensei.conf;

import java.io.File;

import proj.zoie.api.indexing.ZoieIndexableInterpreter;
import proj.zoie.impl.indexing.ZoieConfig;

import com.sensei.search.nodes.SenseiIndexReaderDecorator;
import com.sensei.search.nodes.SenseiZoieFactory;

public interface ZoieFactoryFactory {
	SenseiZoieFactory<?> getZoieFactory(File idxDir,ZoieIndexableInterpreter<?> interpreter,SenseiIndexReaderDecorator decorator,ZoieConfig config);
}
