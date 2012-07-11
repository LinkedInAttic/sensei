package com.senseidb.search.req;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SenseiSystemInfo implements AbstractSenseiResult {

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
    
    @Override
    public String toString(){
      StringBuffer buf = new StringBuffer();
      buf.append("name: ").append(_name);
      buf.append("\nisRuntime: ").append(_runTime);
      buf.append("\nprops: ").append(_props);
      return buf.toString();
    }
  }

  public static class SenseiNodeInfo implements Serializable
  {
    private static final long serialVersionUID = 1L;

    private int _id;
    private int[] _partitions;

    private String _nodeLink;
    private String _adminLink;

    public SenseiNodeInfo(int id, int[] partitions, String nodeLink, String adminLink)
    {
      _id = id;
      _partitions = partitions;
      _nodeLink = nodeLink;
      _adminLink = adminLink;
    }

    public int getId()
    {
      return _id;
    }

    public int[] getPartitions()
    {
      return _partitions;
    }

    public String getNodeLink()
    {
      return _nodeLink;
    }

    public String getAdminLink()
    {
      return _adminLink;
    }

    public String toString()
    {
      StringBuffer buf = new StringBuffer();
      buf.append("{id: ").append(_id)
         .append(", partitions: ").append(Arrays.toString(_partitions))
         .append(", nodeLink: ").append(_nodeLink)
         .append(", adminLink: ").append(_adminLink).append("}");
      return buf.toString();
    }
  }
  
  private static final long serialVersionUID = 1L;

  private long _searchTimeMillis;

  private int _numDocs;
  private long _lastModified;
  private String _version;
  private Set<SenseiFacetInfo> _facetInfos;
  private String _schema; /* JSONObject is not protobuf serializerable, we use string here. */
  private List<SenseiNodeInfo> _clusterInfo;

  private List<SenseiError> errors;
    
  public SenseiSystemInfo(){
    _numDocs = 0;
    _lastModified =0L;
    _version = null;
    _facetInfos = null;
    _schema = null;
    _clusterInfo = null;
  }

  public long getTime() {
    return _searchTimeMillis;
  }

  public void setTime(long searchTimeMillis) {
    _searchTimeMillis = searchTimeMillis;
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

  public String getSchema()
  {
    return _schema;
  }

  public void setSchema(String schema)
  {
    _schema = schema;
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

  public List<SenseiNodeInfo> getClusterInfo() {
    return _clusterInfo;
  }

  public void setClusterInfo(List<SenseiNodeInfo> clusterInfo) {
    _clusterInfo = clusterInfo;
  }
  public List<SenseiError> getErrors() {
    if (errors == null)
      errors = new ArrayList<SenseiError>();

    return errors;
  }

  public void addError(SenseiError error) {
    if (errors == null)
      errors = new ArrayList<SenseiError>();

    errors.add(error);
  }
  @Override
  public String toString(){
    StringBuffer buf = new StringBuffer();
    buf.append("\t- Number of Documents: ").append(_numDocs);
    buf.append("\n\t- Last Modified: ").append(new SimpleDateFormat("EEE, MMM d, ''yy").format(new Date(_lastModified)));
    buf.append("\n\t- Version: ").append(_version);
    if (_schema != null && _schema.length() != 0)
      buf.append("\n\tschema: ").append(_schema);
    buf.append("\n\t- Facet Information: ").append(getCmdOutPutofSet(_facetInfos));
    buf.append("\n\t- Cluster Information: ").append(_clusterInfo);
    return buf.toString();
  }

  /**
   * @param facetInfos
   * @return
   */
  private String getCmdOutPutofSet(Set<SenseiFacetInfo> facetInfos)
  {
    if (facetInfos == null)
      return "null";

    StringBuffer buf = new StringBuffer();
    Iterator<SenseiFacetInfo> it = facetInfos.iterator();
    int count = 0;
    while(it.hasNext()){
      count++;
      SenseiFacetInfo senseiFacetInfo = it.next();
      String _name = senseiFacetInfo.getName();
      boolean _runTime = senseiFacetInfo.isRunTime();
      Map<String, String> _props = senseiFacetInfo.getProps();
      Map<String, String> _sorted_props = sortByKey(_props);
      
      buf.append("\n\t  "+ padString("("+count+")",4)+"name: ").append(_name);
      buf.append("\n\t      isRuntime: ").append(_runTime);
      buf.append("\n\t      props: ").append(_sorted_props);
    }
    return buf.toString();
  }
  
  private String padString(String input, int length){
    if(input.length()>length)
      return input;
    else{
      StringBuffer sb = new StringBuffer(input);
      for(int i=0; i<(length-input.length()); i++){
        sb.append(" ");
      }
      return sb.toString();
    }
  }

  /**
   * @param _props
   */
  private Map<String, String> sortByKey(Map<String, String> _props)
  {
    Map<String, String> map= new LinkedHashMap<String, String>();
    ArrayList<String> sortedKeys = new ArrayList<String>(_props.keySet());
    Collections.sort(sortedKeys);
    for(String key: sortedKeys){
      map.put(key, _props.get(key));
    }
    return map;
  }
  
}
