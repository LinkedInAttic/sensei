package com.sensei.search.nodes;

import java.io.IOException;
import java.util.List;

import org.apache.log4j.Logger;

import proj.zoie.api.ZoieIndexReader;
import proj.zoie.impl.indexing.AbstractIndexReaderDecorator;

import com.browseengine.bobo.api.BoboIndexReader;
import com.browseengine.bobo.facets.FacetHandler;

public class SenseiIndexReaderDecorator extends AbstractIndexReaderDecorator<BoboIndexReader> {
	private final List<FacetHandler<?>> _facetHandlers;
	private static final Logger logger = Logger.getLogger(SenseiIndexReaderDecorator.class);
	
	public SenseiIndexReaderDecorator(List<FacetHandler<?>> facetHandlers)
	{
	  _facetHandlers = facetHandlers;
	}
	
	public SenseiIndexReaderDecorator()
	{
		this(null);
	}
	
	public BoboIndexReader decorate(ZoieIndexReader<BoboIndexReader> zoieReader) throws IOException {
		BoboIndexReader boboReader = null;
        if (zoieReader != null){
          boboReader = BoboIndexReader.getInstanceAsSubReader(zoieReader,_facetHandlers);
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

