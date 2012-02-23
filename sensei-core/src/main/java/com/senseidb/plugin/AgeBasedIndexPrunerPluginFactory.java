package com.senseidb.plugin;

import java.util.Map;

import com.senseidb.indexing.SenseiIndexPruner;

public class AgeBasedIndexPrunerPluginFactory implements SenseiPluginFactory<SenseiIndexPruner> {

	@Override
	public SenseiIndexPruner getBean(Map<String, String> initProperties,
			String fullPrefix, SenseiPluginRegistry pluginRegistry)
	{
		return new SenseiIndexPruner.AgeBasedSelectionSenseiIndexPruner(
					Long.parseLong(initProperties.get("max_age_in_days")));
				
	}

}
