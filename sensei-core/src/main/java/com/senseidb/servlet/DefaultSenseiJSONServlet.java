package com.senseidb.servlet;

import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_COUNT;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_DYNAMIC_INIT;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_DYNAMIC_TYPE;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_DYNAMIC_TYPE_BOOL;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_DYNAMIC_TYPE_BYTEARRAY;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_DYNAMIC_TYPE_DOUBLE;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_DYNAMIC_TYPE_INT;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_DYNAMIC_TYPE_LONG;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_DYNAMIC_TYPE_STRING;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_DYNAMIC_VAL;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_FACET;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_FACET_EXPAND;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_FACET_MAX;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_FACET_MINHIT;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_FACET_ORDER;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_FACET_ORDER_HITS;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_FACET_ORDER_VAL;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_FETCH_STORED;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_FETCH_STORED_VALUE;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_FETCH_TERMVECTOR;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_GROUP_BY;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_MAX_PER_GROUP;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_OFFSET;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_PARTITIONS;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_QUERY;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_QUERY_PARAM;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_RESULT_ERRORS;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_RESULT_ERROR_CODE;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_RESULT_ERROR_MESSAGE;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_RESULT_ERROR_TYPE;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_RESULT_FACETS;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_RESULT_FACET_INFO_COUNT;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_RESULT_FACET_INFO_SELECTED;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_RESULT_FACET_INFO_VALUE;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_RESULT_HITS;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_RESULT_HITS_EXPL_DESC;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_RESULT_HITS_EXPL_DETAILS;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_RESULT_HITS_EXPL_VALUE;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_RESULT_HIT_DOCID;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_RESULT_HIT_EXPLANATION;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_RESULT_HIT_GROUPHITS;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_RESULT_HIT_GROUPHITSCOUNT;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_RESULT_HIT_GROUPFIELD;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_RESULT_HIT_GROUPVALUE;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_RESULT_HIT_SCORE;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_RESULT_HIT_SRC_DATA;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_RESULT_HIT_STORED_FIELDS;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_RESULT_HIT_STORED_FIELDS_NAME;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_RESULT_HIT_STORED_FIELDS_VALUE;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_RESULT_HIT_TERMVECTORS;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_RESULT_HIT_UID;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_RESULT_NUMGROUPS;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_RESULT_NUMHITS;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_RESULT_PARSEDQUERY;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_RESULT_SELECT_LIST;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_RESULT_TID;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_RESULT_TIME;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_RESULT_TOTALDOCS;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_ROUTE_PARAM;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_SELECT;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_SELECT_NOT;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_SELECT_OP;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_SELECT_OP_AND;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_SELECT_OP_OR;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_SELECT_PROP;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_SELECT_VAL;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_SHOW_EXPLAIN;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_SORT;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_SORT_DESC;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_SORT_DOC;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_SORT_DOC_REVERSE;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_SORT_SCORE;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_SORT_SCORE_REVERSE;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_SYSINFO_CLUSTERINFO;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_SYSINFO_CLUSTERINFO_ADMINLINK;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_SYSINFO_CLUSTERINFO_ID;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_SYSINFO_CLUSTERINFO_NODELINK;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_SYSINFO_CLUSTERINFO_PARTITIONS;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_SYSINFO_FACETS;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_SYSINFO_FACETS_NAME;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_SYSINFO_FACETS_PROPS;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_SYSINFO_FACETS_RUNTIME;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_SYSINFO_LASTMODIFIED;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_SYSINFO_NUMDOCS;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_SYSINFO_SCHEMA;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_SYSINFO_VERSION;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.DataConfiguration;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.SortField;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.util.Assert;

import com.browseengine.bobo.api.BrowseFacet;
import com.browseengine.bobo.api.BrowseHit;
import com.browseengine.bobo.api.BrowseSelection;
import com.browseengine.bobo.api.BrowseSelection.ValueOperation;
import com.browseengine.bobo.api.FacetAccessible;
import com.browseengine.bobo.api.FacetSpec;
import com.browseengine.bobo.api.FacetSpec.FacetSortSpec;
import com.browseengine.bobo.facets.DefaultFacetHandlerInitializerParam;
import com.senseidb.search.req.SenseiError;
import com.senseidb.search.req.SenseiHit;
import com.senseidb.search.req.SenseiJSONQuery;
import com.senseidb.search.req.SenseiQuery;
import com.senseidb.search.req.SenseiRequest;
import com.senseidb.search.req.SenseiResult;
import com.senseidb.search.req.SenseiSystemInfo;
import com.senseidb.util.JSONUtil.FastJSONArray;
import com.senseidb.util.JSONUtil.FastJSONObject;
import com.senseidb.util.RequestConverter;


public class DefaultSenseiJSONServlet extends AbstractSenseiRestServlet
{

  private static final String PARAM_RESULT_MAP_REDUCE = "mapReduceResult";

  /**
   *
   */
  private static final long serialVersionUID = 1L;

  private static Logger logger = Logger.getLogger(DefaultSenseiJSONServlet.class);

  public static JSONObject convertExpl(Explanation expl)
      throws JSONException
  {
    JSONObject jsonObject = null;
    if (expl != null)
    {
      jsonObject = new FastJSONObject();
      jsonObject.put(PARAM_RESULT_HITS_EXPL_VALUE, expl.getValue());
      String descr = expl.getDescription();
      jsonObject.put(PARAM_RESULT_HITS_EXPL_DESC, descr == null ? "" : descr);
      Explanation[] details = expl.getDetails();
      if (details != null)
      {
        JSONArray detailArray = new FastJSONArray();
        for (Explanation detail : details)
        {
          JSONObject subObj = convertExpl(detail);
          if (subObj != null)
          {
            detailArray.put(subObj);
          }
        }
        jsonObject.put(PARAM_RESULT_HITS_EXPL_DETAILS, detailArray);
      }
    }

    return jsonObject;
  }

  public static JSONObject convert(Map<String, FacetAccessible> facetValueMap, SenseiRequest req)
      throws JSONException
  {
    JSONObject resMap = new FastJSONObject();
    if (facetValueMap != null)
    {
      Set<Entry<String, FacetAccessible>> entrySet = facetValueMap.entrySet();

      for (Entry<String, FacetAccessible> entry : entrySet)
      {
        String fieldname = entry.getKey();

        BrowseSelection sel = req.getSelection(fieldname);
        HashSet<String> selectedVals = new HashSet<String>();
        if (sel != null)
        {
          String[] vals = sel.getValues();
          if (vals != null && vals.length > 0)
          {
            selectedVals.addAll(Arrays.asList(vals));
          }
        }

        FacetAccessible facetAccessible = entry.getValue();
        List<BrowseFacet> facetList = facetAccessible.getFacets();

        ArrayList<JSONObject> facets = new ArrayList<JSONObject>();

        for (BrowseFacet f : facetList)
        {
          String fval = f.getValue();
          if (fval != null && fval.length() > 0)
          {
            JSONObject fv = new FastJSONObject();
            fv.put(PARAM_RESULT_FACET_INFO_COUNT, f.getFacetValueHitCount());
            fv.put(PARAM_RESULT_FACET_INFO_VALUE, fval);
            fv.put(PARAM_RESULT_FACET_INFO_SELECTED, selectedVals.remove(fval));
            facets.add(fv);
          }
        }

        if (selectedVals.size() > 0)
        {
          // selected vals did not make it in top n
          for (String selectedVal : selectedVals)
          {
            if (selectedVal != null && selectedVal.length() > 0)
            {
              BrowseFacet selectedFacetVal = facetAccessible.getFacet(selectedVal);
              JSONObject fv = new FastJSONObject();
              fv.put(PARAM_RESULT_FACET_INFO_COUNT, selectedFacetVal == null ? 0 : selectedFacetVal.getFacetValueHitCount());
              String fval = selectedFacetVal == null ? selectedVal : selectedFacetVal.getValue();
              fv.put(PARAM_RESULT_FACET_INFO_VALUE, fval);
              fv.put(PARAM_RESULT_FACET_INFO_SELECTED, true);
              facets.add(fv);
            }
          }

          // we need to sort it
          FacetSpec fspec = req.getFacetSpec(fieldname);
          assert fspec != null;
          sortFacets(fieldname, facets, fspec);
        }

        resMap.put(fieldname, facets);
      }
    }
    return resMap;
  }

  private static void sortFacets(String fieldName, ArrayList<JSONObject> facets, FacetSpec fspec) {
    FacetSortSpec sortSpec = fspec.getOrderBy();
    if (FacetSortSpec.OrderHitsDesc.equals(sortSpec))
    {
      Collections.sort(facets, new Comparator<JSONObject>()
      {
        @Override
        public int compare(JSONObject o1, JSONObject o2)
        {
          try
          {
            int c1 = o1.getInt(PARAM_RESULT_FACET_INFO_COUNT);
            int c2 = o2.getInt(PARAM_RESULT_FACET_INFO_COUNT);
            int val = c2 - c1;
            if (val == 0)
            {
              String s1 = o1.getString(PARAM_RESULT_FACET_INFO_VALUE);
              String s2 = o1.getString(PARAM_RESULT_FACET_INFO_VALUE);
              val = s1.compareTo(s2);
            }
            return val;
          }
          catch (Exception e)
          {
            logger.error(e.getMessage(), e);
            return 0;
          }
        }
      });
    }
    else if (FacetSortSpec.OrderValueAsc.equals(sortSpec))
    {
      Collections.sort(facets, new Comparator<JSONObject>()
      {
        @Override
        public int compare(JSONObject o1, JSONObject o2)
        {
          try
          {
            String s1 = o1.getString(PARAM_RESULT_FACET_INFO_VALUE);
            String s2 = o1.getString(PARAM_RESULT_FACET_INFO_VALUE);
            return s1.compareTo(s2);
          }
          catch (Exception e)
          {
            logger.error(e.getMessage(), e);
            return 0;
          }
        }
      });
    }
    else
    {
      throw new IllegalStateException(fieldName + " sorting is not supported");
    }
  }

  @Override
  protected String buildResultString(HttpServletRequest httpReq, SenseiRequest req, SenseiResult res)
      throws Exception
  {
    return supportJsonp(httpReq, buildJSONResultString(req, res));
  }

  private String supportJsonp(HttpServletRequest httpReq, String jsonString) {
    String callback = httpReq.getParameter("callback");
    if (callback != null) {
      return callback + "(" + jsonString + ");";
    } else {
      return jsonString;
    }   
  }

  public static String buildJSONResultString(SenseiRequest req, SenseiResult res)
      throws Exception
  {
    JSONObject jsonObj = buildJSONResult(req, res);
    return jsonObj.toString();
  }

  public static JSONArray buildJSONHits(SenseiRequest req, SenseiHit[] hits)
      throws Exception
  {
    Set<String> selectSet = req.getSelectSet();

    JSONArray hitArray = new FastJSONArray();
    for (SenseiHit hit : hits)
    {
      Map<String, String[]> fieldMap = hit.getFieldValues();

      JSONObject hitObj = new FastJSONObject();
      if (selectSet == null || selectSet.contains(PARAM_RESULT_HIT_UID))
      {
        hitObj.put(PARAM_RESULT_HIT_UID, hit.getUID());
      }
      if (selectSet == null || selectSet.contains(PARAM_RESULT_HIT_DOCID))
      {
        hitObj.put(PARAM_RESULT_HIT_DOCID, hit.getDocid());
      }
      if (selectSet == null || selectSet.contains(PARAM_RESULT_HIT_SCORE))
      {
        hitObj.put(PARAM_RESULT_HIT_SCORE, hit.getScore());
      }
      if (selectSet == null || selectSet.contains(PARAM_RESULT_HIT_GROUPFIELD))
      {
        hitObj.put(PARAM_RESULT_HIT_GROUPFIELD, hit.getGroupField());
      }
      if (selectSet == null || selectSet.contains(PARAM_RESULT_HIT_GROUPVALUE))
      {
        hitObj.put(PARAM_RESULT_HIT_GROUPVALUE, hit.getGroupValue());
      }
      if (selectSet == null || selectSet.contains(PARAM_RESULT_HIT_GROUPHITSCOUNT))
      {
        hitObj.put(PARAM_RESULT_HIT_GROUPHITSCOUNT, hit.getGroupHitsCount());
      }
      if (hit.getGroupHits() != null && hit.getGroupHits().length > 0)
        hitObj.put(PARAM_RESULT_HIT_GROUPHITS, buildJSONHits(req, hit.getSenseiGroupHits()));

      // get fetchStored even if request does not have it because it could be set at the 
      // federated broker level
      if (selectSet == null || selectSet.contains(PARAM_RESULT_HIT_SRC_DATA) || 
          req.isFetchStoredFields() || hit.getSrcData() != null)
      {
        hitObj.put(PARAM_RESULT_HIT_SRC_DATA, hit.getSrcData());
      }
      if (fieldMap != null)
      {
        Set<Entry<String, String[]>> entries = fieldMap.entrySet();
        for (Entry<String, String[]> entry : entries)
        {
          String key = entry.getKey();
          if (key.equals(PARAM_RESULT_HIT_UID))
          {
            // UID is already set.
            continue;
          }
          String[] vals = entry.getValue();

          JSONArray valArray = new FastJSONArray();
          if (vals != null)
          {
            for (String val : vals)
            {
              valArray.put(val);
            }
          }
          if (selectSet == null || selectSet.contains(key))
          {
            hitObj.put(key, valArray);
          }
        }
      }

      Document doc = hit.getStoredFields();
      if (doc != null)
      {
        List<JSONObject> storedData = new ArrayList<JSONObject>();
        List<Fieldable> fields = doc.getFields();
        for (Fieldable field : fields)
        {
          JSONObject data = new FastJSONObject();
          data.put(PARAM_RESULT_HIT_STORED_FIELDS_NAME, field.name());
          data.put(PARAM_RESULT_HIT_STORED_FIELDS_VALUE, field.stringValue());
          storedData.add(data);
        }
        if (selectSet == null || selectSet.contains(PARAM_RESULT_HIT_STORED_FIELDS))
        {
          hitObj.put(PARAM_RESULT_HIT_STORED_FIELDS, new FastJSONArray(storedData));
        }
      }

      Map<String,BrowseHit.TermFrequencyVector> tvMap = hit.getTermFreqMap();
      if (tvMap!=null && tvMap.size()>0){
        JSONObject tvObj = new FastJSONObject();
        if (selectSet == null || selectSet.contains(PARAM_RESULT_HIT_TERMVECTORS))
        {
          hitObj.put(PARAM_RESULT_HIT_TERMVECTORS, tvObj);
        }
        Set<Entry<String,BrowseHit.TermFrequencyVector>> entries = tvMap.entrySet();
        for (Entry<String,BrowseHit.TermFrequencyVector> entry : entries){
          String field = entry.getKey();
          JSONArray tvArray = new FastJSONArray();
          tvObj.put(field, tvArray);
          String[] terms = entry.getValue().terms;
          int[] freqs = entry.getValue().freqs;
          for (int i=0;i<terms.length;++i){
            JSONObject tv = new FastJSONObject();
            tv.put("term", terms[i]);
            tv.put("freq", freqs[i]);
            tvArray.put(tv);
          }
        }
      }

      Explanation expl = hit.getExplanation();
      if (expl != null)
      {
        if (selectSet == null || selectSet.contains(PARAM_RESULT_HIT_EXPLANATION))
        {
          hitObj.put(PARAM_RESULT_HIT_EXPLANATION, convertExpl(expl));
        }
      }

      hitArray.put(hitObj);
    }
    return hitArray;
  }

  public static JSONObject buildJSONResult(SenseiRequest req, SenseiResult res)
      throws Exception
  {
    JSONObject jsonObj = new FastJSONObject();
    jsonObj.put(PARAM_RESULT_TID, res.getTid());
    jsonObj.put(PARAM_RESULT_TOTALDOCS, res.getTotalDocs());
    jsonObj.put(PARAM_RESULT_NUMHITS, res.getNumHits());
    jsonObj.put(PARAM_RESULT_NUMGROUPS, res.getNumGroups());
    jsonObj.put(PARAM_RESULT_PARSEDQUERY, res.getParsedQuery());
    addErrors(jsonObj, res);
    SenseiHit[] hits = res.getSenseiHits();
    JSONArray hitArray = buildJSONHits(req, hits);
    jsonObj.put(PARAM_RESULT_HITS, hitArray);

    List<String> selectList = req.getSelectList();
    if (selectList != null)
    {
      JSONArray jsonSelectList = new FastJSONArray();
      for (String col: selectList)
      {
        jsonSelectList.put(col);
      }
      jsonObj.put(PARAM_RESULT_SELECT_LIST, jsonSelectList);
    }

    jsonObj.put(PARAM_RESULT_TIME, res.getTime());
    jsonObj.put(PARAM_RESULT_FACETS, convert(res.getFacetMap(), req));
    if (req.getMapReduceFunction() != null && res.getMapReduceResult() != null) {
      jsonObj.put(PARAM_RESULT_MAP_REDUCE, req.getMapReduceFunction().render(res.getMapReduceResult().getReduceResult()));
    }
   
    return jsonObj;
  }

  private static void addErrors(JSONObject jsonResult, SenseiResult res) throws JSONException {
    JSONArray errorsJson = new FastJSONArray();
    for (SenseiError error: res.getErrors()) {
      errorsJson.put(new FastJSONObject().put(PARAM_RESULT_ERROR_MESSAGE, error.getMessage())
                                         .put(PARAM_RESULT_ERROR_TYPE, error.getErrorType().name())
                                         .put(PARAM_RESULT_ERROR_CODE, error.getErrorCode()));
    }
    jsonResult.put(PARAM_RESULT_ERRORS, errorsJson);
    if (res.getErrors().size() > 0) {
      jsonResult.put(PARAM_RESULT_ERROR_CODE, res.getErrors().get(0).getErrorCode());
    } else {
      jsonResult.put(PARAM_RESULT_ERROR_CODE, 0);
    }
  }

  private static SenseiQuery buildSenseiQuery(DataConfiguration params)
  {
    SenseiQuery sq;
    String query = params.getString(PARAM_QUERY, null);

    JSONObject qjson = new FastJSONObject();
    if (query != null && query.length() > 0)
    {
      try
      {
        qjson.put("query", query);
      }
      catch (Exception e)
      {
        logger.error(e.getMessage(), e);
      }
    }

    try
    {
      String[] qparams = params.getStringArray(PARAM_QUERY_PARAM);
      for (String qparam : qparams)
      {
        qparam = qparam.trim();
        if (qparam.length() == 0)
          continue;
        String[] parts = qparam.split(":", 2);
        if (parts.length == 2)
        {
          qjson.put(parts[0], parts[1]);
        }
      }
    }
    catch (JSONException jse)
    {
      logger.error(jse.getMessage(), jse);
    }

    sq = new SenseiJSONQuery(qjson);
    return sq;
  }

  @Override
  protected SenseiRequest buildSenseiRequest(DataConfiguration params)
      throws Exception
  {
    return convertSenseiRequest(params);
  }

  public static SenseiRequest convertSenseiRequest(DataConfiguration params)
  {
    SenseiRequest senseiReq = new SenseiRequest();

    convertScalarParams(senseiReq, params);
    convertSenseiQuery(senseiReq, params);
    convertSortParam(senseiReq, params);
    convertSelectParam(senseiReq, params);
    convertFacetParam(senseiReq, params);
    convertInitParams(senseiReq, params);
    convertPartitionParams(senseiReq, params);

    return senseiReq;
  }

  public static void convertSenseiQuery(SenseiRequest senseiReq, DataConfiguration params) {
    senseiReq.setQuery(buildSenseiQuery(params));
  }

  public static void convertScalarParams(SenseiRequest senseiReq, DataConfiguration params) {
    senseiReq.setOffset(params.getInt(PARAM_OFFSET, 0));
    senseiReq.setCount(params.getInt(PARAM_COUNT, 10));
    senseiReq.setShowExplanation(params.getBoolean(PARAM_SHOW_EXPLAIN, false));
    senseiReq.setFetchStoredFields(params.getBoolean(PARAM_FETCH_STORED, false));
    senseiReq.setFetchStoredValue(params.getBoolean(PARAM_FETCH_STORED_VALUE, false));


    String[] fetchTVs= params.getStringArray(PARAM_FETCH_TERMVECTOR);
    if (fetchTVs!=null && fetchTVs.length>0){
      HashSet<String> tvsToFetch = new HashSet<String>(Arrays.asList(fetchTVs));
      tvsToFetch.remove("");
      if (tvsToFetch.size() > 0)
        senseiReq.setTermVectorsToFetch(tvsToFetch);
    }
    String groupBy = params.getString(PARAM_GROUP_BY, null);
    if (groupBy != null && groupBy.length() != 0)
      senseiReq.setGroupBy(StringUtils.split(groupBy, ','));
    senseiReq.setMaxPerGroup(params.getInt(PARAM_MAX_PER_GROUP, 0));
    String routeParam = params.getString(PARAM_ROUTE_PARAM);
    if (routeParam != null && routeParam.length() != 0)
      senseiReq.setRouteParam(routeParam);
  }

  public static void convertPartitionParams(SenseiRequest senseiReq, DataConfiguration params)
  {
    if (params.containsKey(PARAM_PARTITIONS)) {
      List<Integer> partitions = params.getList(Integer.class, PARAM_PARTITIONS);
      senseiReq.setPartitions(new HashSet<Integer>(partitions));
    }
  }

  public static void convertInitParams(SenseiRequest senseiReq, DataConfiguration params)
  {
    Map<String, Configuration> facetParamMap = RequestConverter.parseParamConf(params, PARAM_DYNAMIC_INIT);
    Set<Entry<String, Configuration>> facetEntries = facetParamMap.entrySet();

    for (Entry<String, Configuration> facetEntry : facetEntries)
    {
      String facetName = facetEntry.getKey();
      Configuration facetConf = facetEntry.getValue();

      DefaultFacetHandlerInitializerParam facetParams = new DefaultFacetHandlerInitializerParam();

      Iterator paramsIter = facetConf.getKeys();

      while (paramsIter.hasNext())
      {
        String paramName = (String)paramsIter.next();
        Configuration paramConf = (Configuration)facetConf.getProperty(paramName);

        String type = paramConf.getString(PARAM_DYNAMIC_TYPE);
        List<String> vals = paramConf.getList(PARAM_DYNAMIC_VAL);

        try
        {
          String[] attrVals = vals.toArray(new String[0]);

          if (attrVals.length == 0 || attrVals[0].length() == 0)
          {
            logger.warn(String.format("init param has no values: facet: %s, type: %s", facetName, type));
            continue;
          }

          // TODO: smarter dispatching, factory, generics
          if (type.equalsIgnoreCase(PARAM_DYNAMIC_TYPE_BOOL))
          {
            createBooleanInitParam(facetParams, paramName, attrVals);
          }
          else if (type.equalsIgnoreCase(PARAM_DYNAMIC_TYPE_STRING))
          {
            createStringInitParam(facetParams, paramName, attrVals);
          }
          else if (type.equalsIgnoreCase(PARAM_DYNAMIC_TYPE_INT))
          {
            createIntInitParam(facetParams, paramName, attrVals);
          }
          else if (type.equalsIgnoreCase(PARAM_DYNAMIC_TYPE_BYTEARRAY))
          {
            createByteArrayInitParam(facetParams, paramName, paramConf.getString(PARAM_DYNAMIC_VAL));
          }
          else if (type.equalsIgnoreCase(PARAM_DYNAMIC_TYPE_LONG))
          {
            createLongInitParam(facetParams, paramName, attrVals);
          }
          else if (type.equalsIgnoreCase(PARAM_DYNAMIC_TYPE_DOUBLE))
          {
            createDoubleInitParam(facetParams, paramName, attrVals);
          }
          else
          {
            logger.warn(String.format("Unknown init param name: %s, type %s, for facet: %s", paramName, type, facetName));
            continue;
          }

        }
        catch (Exception e)
        {
          logger.warn(String.format("Failed to parse init param name: %s, type %s, for facet: %s", paramName, type, facetName));
        }
      }

      senseiReq.setFacetHandlerInitializerParam(facetName, facetParams);
    }
  }

  private static void createBooleanInitParam(
      DefaultFacetHandlerInitializerParam facetParams,
      String name,
      String[] paramVals)
  {
    boolean[] vals = new boolean[paramVals.length];
    int i = 0;
    for (String paramVal : paramVals ) {
      vals[i++] = Boolean.parseBoolean(paramVal);
    }

    facetParams.putBooleanParam(name, vals);
  }

  private static void createStringInitParam(
      DefaultFacetHandlerInitializerParam facetParams,
      String name,
      String[] paramVals)
  {
    facetParams.putStringParam(name, Arrays.asList(paramVals));
  }

  private static void createIntInitParam(
      DefaultFacetHandlerInitializerParam facetParams,
      String name,
      String[] paramVals)
  {
    int[] vals = new int[paramVals.length];
    int i = 0;
    for (String paramVal : paramVals ) {
      vals[i++] = Integer.parseInt(paramVal);
    }

    facetParams.putIntParam(name, vals);
  }

  private static void createByteArrayInitParam(
      DefaultFacetHandlerInitializerParam facetParams,
      String name,
      String paramVal)
      throws UnsupportedEncodingException
  {
    byte[] val = paramVal.getBytes("UTF-8");
    facetParams.putByteArrayParam(name, val);
  }

  private static void createLongInitParam(
      DefaultFacetHandlerInitializerParam facetParams,
      String name,
      String[] paramVals)
  {
    long[] vals = new long[paramVals.length];
    int i = 0;
    for (String paramVal : paramVals ) {
      vals[i++] = Long.parseLong(paramVal);
    }

    facetParams.putLongParam(name, vals);
  }

  private static void createDoubleInitParam(
      DefaultFacetHandlerInitializerParam facetParams,
      String name,
      String[] paramVals)
  {
    double[] vals = new double[paramVals.length];
    int i = 0;
    for (String paramVal : paramVals ) {
      vals[i++] = Double.parseDouble(paramVal);
    }

    facetParams.putDoubleParam(name, vals);
  }

  public static void convertSortParam(SenseiRequest senseiReq, DataConfiguration params)
  {
    String[] sortStrings = params.getStringArray(PARAM_SORT);

    if (sortStrings != null && sortStrings.length > 0)
    {
      ArrayList<SortField> sortFieldList = new ArrayList<SortField>(sortStrings.length);

      for (String sortString : sortStrings)
      {
        sortString = sortString.trim();
        if (sortString.length() == 0) continue;
        SortField sf;
        String[] parts = sortString.split(":");
        if (parts.length == 2)
        {
          boolean reverse = PARAM_SORT_DESC.equals(parts[1]);
          sf = new SortField(parts[0], SortField.CUSTOM, reverse);
        }
        else if (parts.length == 1)
        {
          if (PARAM_SORT_SCORE.equals(parts[0]))
          {
            sf = SenseiRequest.FIELD_SCORE;
          }
          else if (PARAM_SORT_SCORE_REVERSE.equals(parts[0]))
          {
            sf = SenseiRequest.FIELD_SCORE_REVERSE;
          }
          else if (PARAM_SORT_DOC.equals(parts[0]))
          {
            sf = SenseiRequest.FIELD_DOC;
          }
          else if (PARAM_SORT_DOC_REVERSE.equals(parts[0]))
          {
            sf = SenseiRequest.FIELD_DOC_REVERSE;
          }
          else
          {
            sf = new SortField(parts[0], SortField.CUSTOM, false);
          }
        }
        else
        {
          throw new IllegalArgumentException("invalid sort string: " + sortString);
        }

        if (sf.getType() != SortField.DOC && sf.getType() != SortField.SCORE &&
            (sf.getField() == null || sf.getField().isEmpty()))   // Empty field name.
          continue;

        sortFieldList.add(sf);
      }

      senseiReq.setSort(sortFieldList.toArray(new SortField[sortFieldList.size()]));
    }
  }

  public static void convertFacetParam(SenseiRequest senseiReq, DataConfiguration params)
  {
    Map<String, Configuration> facetParamMap = RequestConverter.parseParamConf(params, PARAM_FACET);
    Set<Entry<String, Configuration>> entries = facetParamMap.entrySet();

    for (Entry<String, Configuration> entry : entries)
    {
      String name = entry.getKey();
      Configuration conf = entry.getValue();
      FacetSpec fspec = new FacetSpec();

      fspec.setExpandSelection(conf.getBoolean(PARAM_FACET_EXPAND, false));
      fspec.setMaxCount(conf.getInt(PARAM_FACET_MAX, 10));
      fspec.setMinHitCount(conf.getInt(PARAM_FACET_MINHIT, 1));

      FacetSpec.FacetSortSpec orderBy;
      String orderString = conf.getString(PARAM_FACET_ORDER, PARAM_FACET_ORDER_HITS);
      if (PARAM_FACET_ORDER_HITS.equals(orderString))
      {
        orderBy = FacetSpec.FacetSortSpec.OrderHitsDesc;
      }
      else if (PARAM_FACET_ORDER_VAL.equals(orderString))
      {
        orderBy = FacetSpec.FacetSortSpec.OrderValueAsc;
      }
      else
      {
        throw new IllegalArgumentException("invalid order string: " + orderString);
      }
      fspec.setOrderBy(orderBy);
      senseiReq.setFacetSpec(name, fspec);
    }
  }

  public static void convertSelectParam(SenseiRequest senseiReq, DataConfiguration params)
  {
    Map<String, Configuration> selectParamMap = RequestConverter.parseParamConf(params, PARAM_SELECT);
    Set<Entry<String, Configuration>> entries = selectParamMap.entrySet();

    for (Entry<String, Configuration> entry : entries)
    {
      String name = entry.getKey();
      Configuration conf = entry.getValue();

      BrowseSelection sel = new BrowseSelection(name);

      String[] vals = conf.getStringArray(PARAM_SELECT_VAL);
      for (String val : vals)
      {
        if (val.trim().length() > 0)
        {
          sel.addValue(val);
        }
      }

      vals = conf.getStringArray(PARAM_SELECT_NOT);
      for (String val : vals)
      {
        if (val.trim().length() > 0)
        {
          sel.addNotValue(val);
        }
      }

      String op = conf.getString(PARAM_SELECT_OP, PARAM_SELECT_OP_OR);

      ValueOperation valOp;
      if (PARAM_SELECT_OP_OR.equals(op))
      {
        valOp = ValueOperation.ValueOperationOr;
      }
      else if (PARAM_SELECT_OP_AND.equals(op))
      {
        valOp = ValueOperation.ValueOperationAnd;
      }
      else
      {
        throw new IllegalArgumentException("invalid selection operation: " + op);
      }
      sel.setSelectionOperation(valOp);

      String[] selectPropStrings = conf.getStringArray(PARAM_SELECT_PROP);
      if (selectPropStrings != null && selectPropStrings.length > 0)
      {
        Map<String, String> prop = new HashMap<String, String>();
        for (String selProp : selectPropStrings)
        {
          if (selProp.trim().length() == 0) continue;

          String[] parts = selProp.split(":");
          if (parts.length == 2)
          {
            prop.put(parts[0], parts[1]);
          }
          else
          {
            throw new IllegalArgumentException("invalid prop string: " + selProp);
          }
        }
        sel.setSelectionProperties(prop);
      }

      senseiReq.addSelection(sel);
    }
  }

  @Override
  protected String buildResultString(HttpServletRequest httpReq, SenseiSystemInfo info) throws Exception {
    JSONObject jsonObj = new FastJSONObject();
    jsonObj.put(PARAM_SYSINFO_NUMDOCS, info.getNumDocs());
    jsonObj.put(PARAM_SYSINFO_LASTMODIFIED, info.getLastModified());
    jsonObj.put(PARAM_SYSINFO_VERSION, info.getVersion());

    if (info.getSchema() != null && info.getSchema().length() != 0)
    {
      jsonObj.put(PARAM_SYSINFO_SCHEMA, new FastJSONObject(info.getSchema()));
    }

    JSONArray jsonArray = new FastJSONArray();
    jsonObj.put(PARAM_SYSINFO_FACETS, jsonArray);
    Set<SenseiSystemInfo.SenseiFacetInfo> facets = info.getFacetInfos();
    if (facets != null) {
        for (SenseiSystemInfo.SenseiFacetInfo facet : facets) {
          JSONObject facetObj = new FastJSONObject();
          facetObj.put(PARAM_SYSINFO_FACETS_NAME, facet.getName());
          facetObj.put(PARAM_SYSINFO_FACETS_RUNTIME, facet.isRunTime());
          facetObj.put(PARAM_SYSINFO_FACETS_PROPS, facet.getProps());
          jsonArray.put(facetObj);
        }
    }

    jsonArray = new FastJSONArray();
    jsonObj.put(PARAM_SYSINFO_CLUSTERINFO, jsonArray);
    List<SenseiSystemInfo.SenseiNodeInfo> clusterInfo = info.getClusterInfo();
    if (clusterInfo != null)
    {
      for (SenseiSystemInfo.SenseiNodeInfo nodeInfo : clusterInfo)
      {
        JSONObject nodeObj = new FastJSONObject();
        nodeObj.put(PARAM_SYSINFO_CLUSTERINFO_ID, nodeInfo.getId());
        nodeObj.put(PARAM_SYSINFO_CLUSTERINFO_PARTITIONS, new FastJSONArray(Arrays.asList(nodeInfo.getPartitions())));
        nodeObj.put(PARAM_SYSINFO_CLUSTERINFO_NODELINK, nodeInfo.getNodeLink());
        nodeObj.put(PARAM_SYSINFO_CLUSTERINFO_ADMINLINK, nodeInfo.getAdminLink());
        jsonArray.put(nodeObj);
      }
    }

    return supportJsonp(httpReq, jsonObj.toString());
  }
}
