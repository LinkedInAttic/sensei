package com.sensei.search.req;

import java.util.Set;

import com.browseengine.bobo.facets.RuntimeFacetHandler;

public abstract class SenseiRuntimeFacetHandler<D> extends RuntimeFacetHandler<D> implements
		RuntimeInitializable {

	public SenseiRuntimeFacetHandler(String name) {
		super(name);
	}
	
	public SenseiRuntimeFacetHandler(String name, Set<String> dependsOn){
	    super(name, dependsOn);
	}
}
