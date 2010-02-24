package com.sensei.search.req;

import com.browseengine.bobo.facets.FacetHandlerFactory;



public interface RuntimeFacetHandlerFactory<D> extends FacetHandlerFactory<SenseiRuntimeFacetHandler<D>>{
	String getName();
}
