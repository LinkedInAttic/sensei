package com.sensei.search.req;

import com.browseengine.bobo.facets.RuntimeFacetHandler;

public interface RuntimeFacetHandlerFactory
{
	String getName();
	RuntimeFacetHandler<?> get(FacetHandlerInitializerParam params);
}
