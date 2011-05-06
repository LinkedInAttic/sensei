package com.sensei.search.nodes.impl;

import java.util.Map;

import proj.zoie.api.DataProvider;
import proj.zoie.api.Zoie;
import proj.zoie.api.ZoieException;

import com.browseengine.bobo.api.BoboIndexReader;
import com.sensei.search.nodes.SenseiIndexingManager;

public class NoopIndexingManager<D> implements SenseiIndexingManager<D> {

	@Override
	public void initialize(
			Map<Integer, Zoie<BoboIndexReader, D>> zoieSystemMap)
			throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public void shutdown() {
		// TODO Auto-generated method stub

	}

	@Override
	public void start() throws Exception {
		// TODO Auto-generated method stub

	}

  @Override
  public DataProvider getDataProvider()
  {
    return null;
  }

  @Override
  public void syncWithVersion(long timeToWait, String version) throws ZoieException
  {
  }

}
