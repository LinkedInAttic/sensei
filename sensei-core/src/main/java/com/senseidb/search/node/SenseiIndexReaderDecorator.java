package com.senseidb.search.node;

import java.io.IOException;
import java.util.List;

import org.apache.log4j.Logger;

import com.linkedin.zoie.api.ZoieIndexReader;
import com.linkedin.zoie.impl.indexing.AbstractIndexReaderDecorator;

import com.linkedin.bobo.api.BoboIndexReader;
import com.linkedin.bobo.facets.FacetHandler;
import com.linkedin.bobo.facets.RuntimeFacetHandlerFactory;

public class SenseiIndexReaderDecorator extends AbstractIndexReaderDecorator<BoboIndexReader> {
	private final List<FacetHandler<?>> _facetHandlers;
	private static final Logger logger = Logger.getLogger(SenseiIndexReaderDecorator.class);
    private final List<RuntimeFacetHandlerFactory<?,?>> _facetHandlerFactories;

	public SenseiIndexReaderDecorator(List<FacetHandler<?>> facetHandlers, List<RuntimeFacetHandlerFactory<?,?>> facetHandlerFactories)
	{
	  _facetHandlers = facetHandlers;
	  _facetHandlerFactories = facetHandlerFactories;
	}
	
	public SenseiIndexReaderDecorator()
	{
		this(null, null);
	}
	
	public List<FacetHandler<?>> getFacetHandlerList(){
		return _facetHandlers;
	}
	
	public List<RuntimeFacetHandlerFactory<?,?>> getFacetHandlerFactories(){
		return _facetHandlerFactories;
	}
	
	public BoboIndexReader decorate(ZoieIndexReader<BoboIndexReader> zoieReader) throws IOException {
		BoboIndexReader boboReader = null;
        if (zoieReader != null){
          boboReader = BoboIndexReader.getInstanceAsSubReader(zoieReader,_facetHandlers, _facetHandlerFactories);
        }
        return boboReader;
	}
	
	@Override
    public BoboIndexReader redecorate(BoboIndexReader reader, ZoieIndexReader<BoboIndexReader> newReader,boolean withDeletes)
                          throws IOException {
          return reader.copy(newReader);
    }
}

