package com.senseidb.search.req;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;

import org.apache.lucene.search.SortField;
import org.json.JSONObject;

import com.browseengine.bobo.api.BrowseSelection;
import com.browseengine.bobo.api.FacetSpec;
import com.browseengine.bobo.facets.FacetHandlerInitializerParam;
import com.senseidb.search.req.mapred.SenseiMapReduce;
import com.senseidb.util.RequestConverter2;

public  class  SenseiRequest implements AbstractSenseiRequest, Cloneable
{
/**
*
*/
private static final         long       serialVersionUID       =         1L;
  
/**
*       The    transaction   ID
*/
private long   tid           =          -1;

  private HashMap<String,BrowseSelection> _selections;
  private ArrayList<SortField> _sortSpecs;
  private Map<String,FacetSpec> _facetSpecMap;
  private Map<String, Integer> _origFacetSpecMaxCounts;
  private SenseiQuery _query;
  private int _offset;
  private int _count;
  private int _origOffset;
  private int _origCount;
  private boolean _fetchStoredFields;
  private boolean _origFetchStoredFields;
  private boolean _fetchStoredValue;
  private Map<String,FacetHandlerInitializerParam> _facetInitParamMap;
  private Set<Integer> _partitions;
  private boolean _showExplanation;
  private static Random _rand = new Random(System.nanoTime());
  private String _routeParam;
	private String _groupBy;  // TODO: Leave here for backward compatible reason, will remove it later.
	private String[] _groupByMulti;
	private String[] _distinct;
  private int _maxPerGroup;
  private Set<String> _termVectorsToFetch;
  private List<String> _selectList; // Select list (mostly used in BQL) 
  private transient Set<String> _selectSet;
  private SenseiMapReduce mapReduceFunction;
  private List<SenseiError> errors;
  
  public SenseiRequest(){
    _facetInitParamMap = new HashMap<String,FacetHandlerInitializerParam>();
    _selections=new HashMap<String,BrowseSelection>();
    _sortSpecs=new ArrayList<SortField>();
    _facetSpecMap=new HashMap<String,FacetSpec>();
    _fetchStoredFields = false;
    _fetchStoredValue = false;
    _partitions = null;
    _showExplanation = false;
    _routeParam = null;
    _groupBy = null;
    _groupByMulti = null;
    _distinct = null;
    _maxPerGroup = 0;
    _termVectorsToFetch = null;
    _selectList = null;
    _selectSet = null;
  }

  public Set<String> getTermVectorsToFetch(){
    return _termVectorsToFetch;
  }
  
  public void setTermVectorsToFetch(Set<String> termVectorsToFetch){
    _termVectorsToFetch = termVectorsToFetch;
  }
/**
   * Get the transaction ID.
   * @return the transaction ID.
   */
  public final long getTid()
  {
    return tid;
  }

  /**
   * Set the transaction ID;
   * @param tid
   */
  public final void setTid(long tid)
  {
    this.tid = tid;
  }
  
  
  public boolean isShowExplanation() {
    return _showExplanation;
    }

    public void setShowExplanation(boolean showExplanation) {
    _showExplanation = showExplanation;
    }

  public void setPartitions(Set<Integer> partitions){
    _partitions = partitions;
  }
  
  public Set<Integer> getPartitions(){
    return _partitions;
  }

  public void setRouteParam(String routeParam)
  {
    _routeParam = routeParam;
  }

  public String getRouteParam()
  {
    if (_routeParam != null)
      return _routeParam;

    return String.valueOf(_rand.nextInt());
  }

  public void setGroupBy(String[] groupBy)
  {
		_groupByMulti = groupBy;
    if (_groupByMulti != null && _groupByMulti.length != 0)
      _groupBy = _groupByMulti[0];
  }

  public String[] getGroupBy()
  {
    if (_groupByMulti == null && _groupBy != null)
      _groupByMulti = new String[]{_groupBy};

		return _groupByMulti;
  }

  public void setDistinct(String[] distinct)
  {
    _distinct = distinct;
  }

  public String[] getDistinct()
  {
    return _distinct;
  }

  public void setMaxPerGroup(int maxPerGroup)
  {
    _maxPerGroup = maxPerGroup;
  }

  public int getMaxPerGroup()
  {
    return _maxPerGroup;
  }

  public Map<String,FacetHandlerInitializerParam> getFacetHandlerInitParamMap(){
    return _facetInitParamMap;
  }
  
  public void setFacetHandlerInitParamMap(Map<String,FacetHandlerInitializerParam> paramMap){
    _facetInitParamMap = paramMap;
  }

  public void putAllFacetHandlerInitializerParams(Map<String,FacetHandlerInitializerParam> params){
    _facetInitParamMap.putAll(params);
  }
  
  public void setFacetHandlerInitializerParam(String name,FacetHandlerInitializerParam param){
    _facetInitParamMap.put(name, param);
  }
  
  public FacetHandlerInitializerParam getFacetHandlerInitializerParam(String name){
    return _facetInitParamMap.get(name);
  }

  public Set<String> getSelectionNames(){
    return _selections.keySet();
  }
  
  public void removeSelection(String name){
    _selections.remove(name);
  }
  
  public void setFacetSpecs(Map<String,FacetSpec> facetSpecMap)
  {
    _facetSpecMap = facetSpecMap;
  }
  
  public Map<String,FacetSpec> getFacetSpecs()
  {
    return _facetSpecMap;
  }
  
  public void saveState()
  {
    _origOffset = _offset;
    _origCount = _count;
    _origFetchStoredFields = _fetchStoredFields;
    if (_origFacetSpecMaxCounts == null && _facetSpecMap != null)
    {
      _origFacetSpecMaxCounts= new HashMap<String, Integer>();
      for (Map.Entry<String, FacetSpec> entry : _facetSpecMap.entrySet())
      {
        FacetSpec spec = entry.getValue();
        if (spec != null)
        {
          _origFacetSpecMaxCounts.put(entry.getKey(), spec.getMaxCount());
        }
      }
    }
  }

  public void restoreState()
  {
    _offset = _origOffset;
    _count = _origCount;
    _fetchStoredFields = _origFetchStoredFields;
    if (_facetSpecMap != null)
    {
      for (Map.Entry<String, FacetSpec> entry : _facetSpecMap.entrySet())
      {
        FacetSpec spec = entry.getValue();
        if (spec != null)
        {
          spec.setMaxCount(_origFacetSpecMaxCounts.get(entry.getKey()));
        }
      }
    }
  }

  public int getSelectionCount()
  {
    return _selections.size();
  }
  
  public void clearSelections(){
    _selections.clear();
  }
  
  /**
   * Gets the number of facet specs
   * @return number of facet pecs
   * @see #setFacetSpec(String, FacetSpec)
   * @see #getFacetSpec(String)
   */
  public int getFacetSpecCount(){
    return _facetSpecMap.size();
  }
  
  public void clearSort(){
    _sortSpecs.clear();
  }
  
  public boolean isFetchStoredFields(){
    return _fetchStoredFields;
  }
  
  public void setFetchStoredFields(boolean fetchStoredFields){
    _fetchStoredFields = fetchStoredFields;
  }
  
  public boolean isFetchStoredValue(){
    return _fetchStoredValue;
  }
  
  public void setFetchStoredValue(boolean fetchStoredValue){
    _fetchStoredValue = fetchStoredValue;
  }

  /**
   * Sets a facet spec
   * @param name field name
   * @param facetSpec Facet spec
   * @see #getFacetSpec(String)
   */
  public void setFacetSpec(String name,FacetSpec facetSpec){
    _facetSpecMap.put(name,facetSpec);
  }
  
  /**
   * Gets a facet spec
   * @param name field name
   * @return facet spec
   * @see #setFacetSpec(String, FacetSpec)
   */
  public FacetSpec getFacetSpec(String name){
    return _facetSpecMap.get(name);
  }
  
  /**
   * Gets the number of hits to return. Part of the paging parameters.
   * @return number of hits to return.
   * @see #setCount(int)
   */
  public int getCount() {
    return _count;
  }

  /**
   * Sets the number of hits to return. Part of the paging parameters.
   * @param count number of hits to return.
   * @see #getCount()
   */
  public void setCount(int count) {
    _count = count;
  }

  /**
   * Gets the offset. Part of the paging parameters.
   * @return offset
   * @see #setOffset(int)
   */
  public int getOffset() {
    return _offset;
  }

  /**
   * Sets of the offset. Part of the paging parameters.
   * @param offset offset
   * @see #getOffset()
   */
  public void setOffset(int offset) {
    _offset = offset;
  }

  /**
   * Set the search query
   * @param query query object
   * @see #getQuery()
   */
  public void setQuery(SenseiQuery query){
    _query=query;
  }
  
  /**
   * Gets the search query
   * @return query object
   * @see #setQuery(SenseiQuery)
   */
  public SenseiQuery getQuery(){
    return _query;
  }

  /**
   * Adds a browse selection array
   * @param selections selections to add
   * @see #addSelection(BrowseSelection)
   * @see #getSelections()
   */
  public void addSelections(BrowseSelection[] selections) {
    for (BrowseSelection selection : selections) {
      addSelection(selection);
    }
  }

  /**
   * Adds a browse selection
   * @param sel selection
   * @see #getSelections()
   */
  public void addSelection(BrowseSelection sel){
    _selections.put(sel.getFieldName(),sel);
  }
  
  /**
   * Gets all added browse selections
   * @return added selections
   * @see #addSelection(BrowseSelection)
   */
  public BrowseSelection[] getSelections(){
    return _selections.values().toArray(new BrowseSelection[_selections.size()]);
  }
  
  /**
   * Gets selection by field name
   * @param fieldname
   * @return selection on the field
   */
  public BrowseSelection getSelection(String fieldname){
    return _selections.get(fieldname);
  }
  
  /**
   * Add a sort spec
   * @param sortSpec sort spec
   * @see #getSort() 
   * @see #setSort(SortField[])
   */
  public void addSortField(SortField sortSpec){
    _sortSpecs.add(sortSpec);
  }

  /**
   * Add a sort spec
   * @param sortSpecs sort spec
   * @see #getSort()
   * @see #setSort(SortField[])
   */
  public void addSortFields(SortField[] sortSpecs){
    for (SortField field : sortSpecs) {
      addSortField(field);
    }
  }

  /**
   * Gets the sort criteria
   * @return sort criteria
   * @see #setSort(SortField[])
   * @see #addSortField(SortField)
   */
  public SortField[] getSort(){
    return _sortSpecs.toArray(new SortField[_sortSpecs.size()]);
  }
  
  /**
   * Sets the sort criteria
   * @param sorts sort criteria
   * @see #addSortField(SortField)
   * @see #getSort()
   */
  public void setSort(SortField[] sorts){
    _sortSpecs.clear();
    for (int i=0;i<sorts.length;++i){
      _sortSpecs.add(sorts[i]);
    }
  }

  /**
   * Sets the select list.
   * @param selectList select list
   */
  public void setSelectList(List<String> selectList)
  {
    _selectList = selectList;
    _selectSet = null;
  }

  /**
   * Gets the select list.
   * @return select list.
   */
  public List<String> getSelectList()
  {
    return _selectList;
  }

  public Set<String> getSelectSet()
  {
    if (_selectSet == null &&
        _selectList != null &&
        !(_selectList.size() == 1 && "*".equals(_selectList.get(0))))
    {
      _selectSet = new HashSet<String>(_selectList);
    }
    return _selectSet;
  }
  
  /** Represents sorting by document score (relevancy). */
  public static final SortField FIELD_SCORE = new SortField (null, SortField.SCORE);
  public static final SortField FIELD_SCORE_REVERSE = new SortField (null, SortField.SCORE, true);

  /** Represents sorting by document number (index order). */
  public static final SortField FIELD_DOC = new SortField (null, SortField.DOC);
  public static final SortField FIELD_DOC_REVERSE = new SortField (null, SortField.DOC, true);

  @Override
  public String toString(){
    StringBuilder buf=new StringBuilder();
    if(_query != null)
      buf.append("query: ").append(_query.toString()).append('\n');
    buf.append("page: [").append(_offset).append(',').append(_count).append("]\n");
    if(_sortSpecs != null)
      buf.append("sort spec: ").append(_sortSpecs).append('\n');
    if(_selections != null)
      buf.append("selections: ").append(_selections).append('\n');
    if(_facetSpecMap != null)
      buf.append("facet spec: ").append(_facetSpecMap).append('\n');
    if (_routeParam != null)
      buf.append("route param: ").append(_routeParam).append('\n');
    if (_groupBy != null)
      buf.append("group by: ").append(_groupBy).append('\n');
    buf.append("max per group: ").append(_maxPerGroup).append('\n');
    buf.append("fetch stored fields: ").append(_fetchStoredFields).append('\n');
    buf.append("fetch stored value: ").append(_fetchStoredValue);
    return buf.toString();
  }

  @Override
  public SenseiRequest clone() {
    SenseiRequest clone = new SenseiRequest();
    clone.setTid(this.getTid());
    
    BrowseSelection[] selections = this.getSelections();
    for(BrowseSelection selection : selections)
      clone.addSelection(selection);
    
    for(SortField sort : this.getSort())
      clone.addSortField(sort);
    
    
    Map<String, FacetSpec> cloneFacetSpecs = new HashMap<String, FacetSpec>();
    for(Entry<String, FacetSpec> facetSpec : this.getFacetSpecs().entrySet()) {
      cloneFacetSpecs.put(facetSpec.getKey(), facetSpec.getValue().clone());
    }
    
    clone.setFacetSpecs(cloneFacetSpecs);
    clone.setQuery(this.getQuery());
    clone.setOffset(this.getOffset());
    clone.setCount(this.getCount());
    clone.setFetchStoredFields(this.isFetchStoredFields());
    clone.setFetchStoredValue(this.isFetchStoredValue());
    clone.setFacetHandlerInitParamMap(this.getFacetHandlerInitParamMap());
    clone.setPartitions(this.getPartitions());
    clone.setShowExplanation(this.isShowExplanation());
    clone.setRouteParam(this.getRouteParam());
    clone.setGroupBy(this.getGroupBy());
    clone.setDistinct(this.getDistinct());
    clone.setMaxPerGroup(this.getMaxPerGroup());
    clone.setTermVectorsToFetch(this.getTermVectorsToFetch());
    if (this.getSelectList() != null) {
      clone.setSelectList(new ArrayList<String>(this.getSelectList()));
    }
    clone.setMapReduceFunction(this.getMapReduceFunction());

    return clone;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof SenseiRequest)) return false;
    SenseiRequest b = (SenseiRequest)o;

    if (getCount() != b.getCount()) return false;
    if (getOffset() != b.getOffset()) return false;
    if (!facetSpecsAreEqual(getFacetSpecs(), b.getFacetSpecs())) return false;
    if (!selectionsAreEqual(getSelections(), b.getSelections())) return false;
    if (!initParamsAreEqual(getFacetHandlerInitParamMap(), b.getFacetHandlerInitParamMap())) return false;
    if (!Arrays.equals(getSort(), b.getSort())) return false;
    if (getQuery() == null) {
      if (b.getQuery() != null) return false;
    } else {
      if (!getQuery().toString().equals(b.getQuery().toString())) return false;
    }
    if (getGroupBy() == null) {
      if (b.getGroupBy() != null) return false;
    }
    else {
      if (!getGroupBy().equals(b.getGroupBy())) return false;
    }
    if (getMaxPerGroup() != b.getMaxPerGroup())
      return false;
    if (getPartitions() == null) {
      if (b.getPartitions() != null) return false;
    } else {
      if (!setsAreEqual(getPartitions(), b.getPartitions())) return false;
    }

    return true;
  }

  private boolean initParamsAreEqual(Map<String, FacetHandlerInitializerParam> a,
                                     Map<String, FacetHandlerInitializerParam> b) {
    if (a.size() != b.size()) return false;

    for (Entry<String,FacetHandlerInitializerParam> entry : a.entrySet()) {
      String key = entry.getKey();
      if (!b.containsKey(key)) return false;
      if (!areFacetHandlerInitializerParamsEqual(entry.getValue(), b.get(key))) return false;
    }

    return true;
  }

  private boolean areFacetHandlerInitializerParamsEqual(FacetHandlerInitializerParam a, FacetHandlerInitializerParam b) {
    if (!setsAreEqual(a.getBooleanParamNames(), b.getBooleanParamNames())) return false;
    if (!setsAreEqual(a.getIntParamNames(), b.getIntParamNames())) return false;
    if (!setsAreEqual(a.getDoubleParamNames(), b.getDoubleParamNames())) return false;
    if (!setsAreEqual(a.getLongParamNames(), b.getLongParamNames())) return false;
    if (!setsAreEqual(a.getStringParamNames(), b.getStringParamNames())) return false;
    if (!setsAreEqual(a.getByteArrayParamNames(), b.getByteArrayParamNames())) return false;

    for (String name : a.getBooleanParamNames()) {
      if (!Arrays.equals(a.getBooleanParam(name), b.getBooleanParam(name))) return false;
    }
    for (String name : a.getIntParamNames()) {
      if (!Arrays.equals(a.getIntParam(name), b.getIntParam(name))) return false;
    }
    for (String name : a.getDoubleParamNames()) {
      if (!Arrays.equals(a.getDoubleParam(name), b.getDoubleParam(name))) return false;
    }
    for (String name : a.getLongParamNames()) {
      if (!Arrays.equals(a.getLongParam(name), b.getLongParam(name))) return false;
    }
    for (String name : a.getStringParamNames()) {
      if (!Arrays.equals(a.getStringParam(name).toArray(new String[0]), b.getStringParam(name).toArray(new String[0]))) return false;
    }
/* NOT YET SUPPORTED
    for (String name : a.getByteArrayParamNames()) {
      assertTrue(Arrays.equals(a.getByteArrayParam(name), b.getByteArrayParam(name)));
    }
*/
    return true;
  }

  private boolean facetSpecsAreEqual(Map<String, FacetSpec> a, Map<String, FacetSpec> b) {
    if (a.size() != b.size()) return false;

    for (Entry<String,FacetSpec> entry : a.entrySet()) {
      String key = entry.getKey();
      if (!(b.containsKey(key))) return false;
      if (!facetSpecsAreEqual(entry.getValue(), b.get(key))) return false;
    }

    return true;
  }

  private boolean facetSpecsAreEqual(FacetSpec a, FacetSpec b) {
    return
        (a.getMaxCount() == b.getMaxCount())
        && (a.getMinHitCount() == b.getMinHitCount())
        && (a.getOrderBy() == b.getOrderBy())
        && (a.isExpandSelection() == b.isExpandSelection());
  }

  private boolean selectionsAreEqual(BrowseSelection[] a, BrowseSelection[] b) {
    if (a.length != b.length) return false;

    for (int i = 0; i < a.length; i++) {
      if (!selectionsAreEqual(a[i], b[i])) return false;
    }

    return true;
  }

  private boolean selectionsAreEqual(BrowseSelection a, BrowseSelection b) {
    return
        (a.getFieldName().equals(b.getFieldName()))
        && (Arrays.equals(a.getValues(), b.getValues()))
        && (Arrays.equals(a.getNotValues(), b.getNotValues()))
        && (a.getSelectionOperation().equals(b.getSelectionOperation()))
        && (a.getSelectionProperties().equals(b.getSelectionProperties()));
  }
  
 

  public SenseiMapReduce getMapReduceFunction() {
    return mapReduceFunction;
  }

  public void setMapReduceFunction(SenseiMapReduce mapReduceFunction) {
    this.mapReduceFunction = mapReduceFunction;
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

  private <T> boolean setsAreEqual(Set<T> a, Set<T> b) {
    if (a.size() != b.size()) return false;

    Iterator<T> iter = a.iterator();
    while (iter.hasNext()) {
      T val = iter.next();
      if (!b.contains(val)) return false;
    }

    return true;
  }
  
  /**
   * Builds SenseiRequest based on a JSON object.
   *
   * @param json  The input JSON object.
   * @param facetInfoMap  Facet information map, which maps a facet name
   *        to a String array in which the first element is the facet
   *        type (like "simple" or "range") and the second element is
   *        the data type (like "int" or "long").
   * @return The built SenseiRequest.
   */
  public static SenseiRequest fromJSON(final JSONObject json,
                                       final Map<String, String[]> facetInfoMap)
    throws Exception
  {
    return RequestConverter2.fromJSON(json, facetInfoMap);
  }

}
