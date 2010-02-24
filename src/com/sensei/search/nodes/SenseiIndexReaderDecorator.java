package com.sensei.search.nodes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import proj.zoie.api.ZoieIndexReader;
import proj.zoie.impl.indexing.AbstractIndexReaderDecorator;

import com.browseengine.bobo.api.BoboIndexReader;
import com.browseengine.bobo.facets.FacetHandler;
import com.browseengine.bobo.facets.FacetHandlerFactory;

public class SenseiIndexReaderDecorator extends AbstractIndexReaderDecorator<BoboIndexReader> {
	private final List<FacetHandlerFactory<?>> _facetHandlerFactories;
	private static final Logger logger = Logger.getLogger(SenseiIndexReaderDecorator.class);
	
	public SenseiIndexReaderDecorator(List<FacetHandlerFactory<?>> facetHandlerFactories)
	{
	  _facetHandlerFactories = facetHandlerFactories;
	}
	
	public SenseiIndexReaderDecorator()
	{
		this(null);
	}
	
	public BoboIndexReader decorate(ZoieIndexReader<BoboIndexReader> zoieReader) throws IOException {
		BoboIndexReader boboReader = null;
        if (zoieReader != null){
          ArrayList<FacetHandler<?>> handerList = null;
          if (_facetHandlerFactories!=null)
          {
            handerList = new ArrayList<FacetHandler<?>>(_facetHandlerFactories.size());
            for (FacetHandlerFactory<?> factory : _facetHandlerFactories)
            {
              FacetHandler<?> handler = (FacetHandler<?>)factory.newInstance();
              handerList.add(handler);
            }
          }
          boboReader = BoboIndexReader.getInstanceAsSubReader(zoieReader,handerList);
        }
        return boboReader;
	}
	
	@Override
    public BoboIndexReader redecorate(BoboIndexReader reader, ZoieIndexReader<BoboIndexReader> newReader)
                          throws IOException {
                  reader.rewrap(newReader);
                  return reader;
    }
}

