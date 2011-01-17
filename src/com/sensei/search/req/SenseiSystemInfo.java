package com.sensei.search.req;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SenseiSystemInfo implements Serializable {

	public static class SenseiFacetInfo implements Serializable{
		private static final long serialVersionUID = 1L;
		
		private String _name;
		private boolean _runTime;
		private Map<String,String> _props;

		public SenseiFacetInfo(String name) {
			_name = name;
			_runTime = false;
			_props = null;
		}
		
		public String getName() {
			return _name;
		}
		public void setName(String name) {
			_name = name;
		}
		public boolean isRunTime() {
			return _runTime;
		}
		public void setRunTime(boolean runTime) {
			_runTime = runTime;
		}
		public Map<String, String> getProps() {
			return _props;
		}
		public void setProps(Map<String, String> props) {
			_props = props;
		}
	}
	
	private static final long serialVersionUID = 1L;
	
	private int _numDocs;
	private long _lastModified;
	private String _version;
		private Set<SenseiFacetInfo> _facetInfos;
		private Map<Integer,List<Integer>> _clusterInfo;
		
	public SenseiSystemInfo(){
		_numDocs = 0;
		_lastModified =0L;
		_version = "0";
		_facetInfos = null;
		_clusterInfo = null;
	}

	public int getNumDocs() {
		return _numDocs;
	}
	
	public void setNumDocs(int numDocs) {
		_numDocs = numDocs;
	}
	
	public long getLastModified() {
		return _lastModified;
	}
	
	public void setLastModified(long lastModified) {
		_lastModified = lastModified;
	}
	
	public Set<SenseiFacetInfo> getFacetInfos() {
		return _facetInfos;
	}
	
	public void setFacetInfos(Set<SenseiFacetInfo> facetInfos) {
		_facetInfos = facetInfos;
	}

	public String getVersion() {
		return _version;
	}

	public void setVersion(String version) {
		_version = version;
	}

	public Map<Integer, List<Integer>> getClusterInfo() {
		return _clusterInfo;
	}

	public void setClusterInfo(Map<Integer, List<Integer>> clusterInfo) {
		_clusterInfo = clusterInfo;
	}
}
