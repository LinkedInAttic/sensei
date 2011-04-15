package com.sensei.dataprovider.http;

import java.util.Comparator;

import proj.zoie.api.ZoieVersion;
import proj.zoie.api.ZoieVersionFactory;

public class StringZoieVersion extends ZoieVersion{
    private final String _version;
    private final Comparator<String> _comparator;
	public StringZoieVersion(String version,Comparator<String> comparator){
		_version = version;
		_comparator = comparator;
	}
	

	public StringZoieVersion(String version){
		this(version,new Comparator<String>(){
			@Override
			public int compare(String o1, String o2) {
				if (o1==null && o2==null){
					return 0;
				}
				if (o1==null) return -1;
				if (o2==null) return 1;
				return o1.compareTo(o2);
			}
		});
	}
	
	@Override
	public String encodeToString() {
		return _version;
	}

	@Override
	public int compareTo(ZoieVersion zversion) {
		return _comparator.compare(_version, ((StringZoieVersion)zversion)._version);
	}
}
