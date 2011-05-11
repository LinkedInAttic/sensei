package com.sensei.search.req;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.lucene.search.SortField;

import com.browseengine.bobo.api.BrowseSelection;
import com.browseengine.bobo.api.FacetSpec;
import com.browseengine.bobo.facets.FacetHandlerInitializerParam;

public class SenseiRequest implements AbstractSenseiRequest, Cloneable
{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
  /**
   * The transaction ID
   */
  private long tid = -1;

  private HashMap<String,BrowseSelection> _selections;
	private ArrayList<SortField> _sortSpecs;
	private Map<String,FacetSpec> _facetSpecMap;
	private Map<String, Integer> _origFacetSpecMaxCounts;
	private SenseiQuery _query;
	private int _offset;
	private int _count;
	private boolean _fetchStoredFields;
	private Map<String,FacetHandlerInitializerParam> _facetInitParamMap;
	private Set<Integer> _partitions;
	private boolean _showExplanation;
	private Random _rand = new Random(System.nanoTime());
	private String _routeParam;
	
	public SenseiRequest(){
		_facetInitParamMap = new HashMap<String,FacetHandlerInitializerParam>();
		_selections=new HashMap<String,BrowseSelection>();
		_sortSpecs=new ArrayList<SortField>();
		_facetSpecMap=new HashMap<String,FacetSpec>();
		_fetchStoredFields = false;
		_partitions = null;
		_showExplanation = false;
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
	
  public void saveOrigFacetMaxCounts()
  {
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

	public void restoreOrigFacetMaxCounts()
  {
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
      buf.append("fetch stored fields: ").append(_fetchStoredFields);
      return buf.toString();
	}
	
	public Object clone() throws CloneNotSupportedException
	{
	  return super.clone();
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

    for (String key : a.keySet()) {
      if (!b.containsKey(key)) return false;
      if (!areFacetHandlerInitializerParamsEqual(a.get(key), b.get(key))) return false;
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

    for (String key : a.keySet()) {
      if (!(b.containsKey(key))) return false;
      if (!facetSpecsAreEqual(a.get(key), b.get(key))) return false;
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

  private <T> boolean setsAreEqual(Set<T> a, Set<T> b) {
    if (a.size() != b.size()) return false;

    Iterator iter = a.iterator();
    while (iter.hasNext()) {
      T val = (T)iter.next();
      if (!b.contains(val)) return false;
    }

    return true;
  }

}
