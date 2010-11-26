package com.sensei.search.client.servlet;

import static com.sensei.search.client.servlet.SenseiSearchServletParams.PARAM_COUNT;
import static com.sensei.search.client.servlet.SenseiSearchServletParams.PARAM_FACET;
import static com.sensei.search.client.servlet.SenseiSearchServletParams.PARAM_FACET_EXPAND;
import static com.sensei.search.client.servlet.SenseiSearchServletParams.PARAM_FACET_MAX;
import static com.sensei.search.client.servlet.SenseiSearchServletParams.PARAM_FACET_MINHIT;
import static com.sensei.search.client.servlet.SenseiSearchServletParams.PARAM_FACET_ORDER;
import static com.sensei.search.client.servlet.SenseiSearchServletParams.PARAM_FACET_ORDER_HITS;
import static com.sensei.search.client.servlet.SenseiSearchServletParams.PARAM_FACET_ORDER_VAL;
import static com.sensei.search.client.servlet.SenseiSearchServletParams.PARAM_FETCH_STORED;
import static com.sensei.search.client.servlet.SenseiSearchServletParams.PARAM_OFFSET;
import static com.sensei.search.client.servlet.SenseiSearchServletParams.PARAM_QUERY;
import static com.sensei.search.client.servlet.SenseiSearchServletParams.PARAM_RESULT_FACETS;
import static com.sensei.search.client.servlet.SenseiSearchServletParams.PARAM_RESULT_HITS;
import static com.sensei.search.client.servlet.SenseiSearchServletParams.PARAM_RESULT_HIT_EXPLANATION;
import static com.sensei.search.client.servlet.SenseiSearchServletParams.PARAM_RESULT_HIT_SCORE;
import static com.sensei.search.client.servlet.SenseiSearchServletParams.PARAM_RESULT_HIT_UID;
import static com.sensei.search.client.servlet.SenseiSearchServletParams.PARAM_RESULT_NUMHITS;
import static com.sensei.search.client.servlet.SenseiSearchServletParams.PARAM_RESULT_PARSEDQUERY;
import static com.sensei.search.client.servlet.SenseiSearchServletParams.PARAM_RESULT_TIME;
import static com.sensei.search.client.servlet.SenseiSearchServletParams.PARAM_RESULT_TOTALDOCS;
import static com.sensei.search.client.servlet.SenseiSearchServletParams.PARAM_SELECT;
import static com.sensei.search.client.servlet.SenseiSearchServletParams.PARAM_SELECT_NOT;
import static com.sensei.search.client.servlet.SenseiSearchServletParams.PARAM_SELECT_OP;
import static com.sensei.search.client.servlet.SenseiSearchServletParams.PARAM_SELECT_OP_AND;
import static com.sensei.search.client.servlet.SenseiSearchServletParams.PARAM_SELECT_OP_OR;
import static com.sensei.search.client.servlet.SenseiSearchServletParams.PARAM_SELECT_PROP;
import static com.sensei.search.client.servlet.SenseiSearchServletParams.PARAM_SELECT_VAL;
import static com.sensei.search.client.servlet.SenseiSearchServletParams.PARAM_SHOW_EXPLAIN;
import static com.sensei.search.client.servlet.SenseiSearchServletParams.PARAM_SORT;
import static com.sensei.search.client.servlet.SenseiSearchServletParams.PARAM_SORT_DESC;
import static com.sensei.search.client.servlet.SenseiSearchServletParams.PARAM_SORT_SCORE;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.DataConfiguration;
import org.apache.log4j.Logger;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.SortField;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.browseengine.bobo.api.BrowseFacet;
import com.browseengine.bobo.api.BrowseSelection;
import com.browseengine.bobo.api.FacetAccessible;
import com.browseengine.bobo.api.FacetSpec;
import com.browseengine.bobo.api.BrowseSelection.ValueOperation;
import com.browseengine.bobo.api.FacetSpec.FacetSortSpec;
import com.sensei.search.req.SenseiHit;
import com.sensei.search.req.SenseiQuery;
import com.sensei.search.req.SenseiRequest;
import com.sensei.search.req.SenseiResult;
import com.sensei.search.req.StringQuery;
import com.sensei.search.util.RequestConverter;


public class DefaultSenseiJSONServlet extends AbstractSenseiRestServlet {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private static Logger logger = Logger.getLogger(DefaultSenseiJSONServlet.class);
	
	public static JSONObject convertExpl(Explanation expl) throws JSONException{
		JSONObject jsonObject = null;
		if (expl!=null){
		  jsonObject = new JSONObject();
		  float val = expl.getValue();
		  jsonObject.put("value", val);
		  String descr = expl.getDescription();
		  jsonObject.put("description", descr==null ? "" : descr);
		  Explanation[] details = expl.getDetails();
		  if (details!=null){
			  JSONArray detailArray = new JSONArray();
			  for (Explanation detail : details){
				  JSONObject subObj = convertExpl(detail);
				  if (subObj!=null){
					  detailArray.put(subObj);
				  }
			  }
			  jsonObject.put("details", detailArray);
		  }
		}
		
		return jsonObject;
	}
	
	public static JSONObject convert(Map<String,FacetAccessible> facetValueMap,SenseiRequest req) throws JSONException{
		JSONObject resMap = new JSONObject();
		if (facetValueMap!=null){
			Set<Entry<String,FacetAccessible>> entrySet = facetValueMap.entrySet();
			for (Entry<String,FacetAccessible> entry : entrySet){
				String fieldname = entry.getKey();
				BrowseSelection sel = req.getSelection(fieldname);
				HashSet<String> selectedVals = new HashSet<String>();
				if (sel!=null){
					String[] vals = sel.getValues();
					if (vals!=null && vals.length>0){
						selectedVals.addAll(Arrays.asList(vals));
					}
				}
				FacetAccessible facetAccessible = entry.getValue();
				List<BrowseFacet> facetList = facetAccessible.getFacets();
				
				ArrayList<JSONObject> facets = new ArrayList<JSONObject>();
				for (BrowseFacet f : facetList){
					String fval = f.getValue();
					if (fval!=null && fval.length()>0){
					  JSONObject fv = new JSONObject();
					  fv.put("count",f.getFacetValueHitCount());
					  fv.put("value",fval);
					  fv.put("selected",selectedVals.remove(fval));
					  facets.add(fv);
					}
				}
				
				if (selectedVals.size()>0){
					// selected vals did not make it in top n
					for (String selectedVal : selectedVals){
					  if (selectedVal != null && selectedVal.length()>0){
					    BrowseFacet selectedFacetVal = facetAccessible.getFacet(selectedVal);
					    JSONObject fv = new JSONObject();
					    fv.put("count",selectedFacetVal==null?0:selectedFacetVal.getFacetValueHitCount());
					    String fval = selectedFacetVal==null?selectedVal:selectedFacetVal.getValue();
					    fv.put("value",fval);
					    fv.put("selected",true);
					    facets.add(fv);
					  }
					}
					
					// we need to sort it
					FacetSpec fspec = req.getFacetSpec(fieldname);
					assert fspec!=null;
					FacetSortSpec sortSpec = fspec.getOrderBy();
					if (FacetSortSpec.OrderHitsDesc.equals(sortSpec)){
						Collections.sort(facets, new Comparator<JSONObject>(){
	
							@Override
							public int compare(JSONObject o1, JSONObject o2) {
								try{
								  int c1 = o1.getInt("count");
								  int c2 = o2.getInt("count");
								  int val = c2 - c1;
								  if (val == 0){
									  String s1 = o1.getString("value");
									  String s2 = o1.getString("value");
									  val = s1.compareTo(s2);
								  }
								  return val;
								}
								catch(Exception e){
									logger.error(e.getMessage(),e);
									return 0;
								}
							}
							
						});
					}
					else if (FacetSortSpec.OrderValueAsc.equals(sortSpec)){
						Collections.sort(facets, new Comparator<JSONObject>(){
	
							@Override
							public int compare(JSONObject o1, JSONObject o2) {
								try{
								  String s1 = o1.getString("value");
								  String s2 = o1.getString("value");
								  return s1.compareTo(s2);
								}
								catch(Exception e){
									logger.error(e.getMessage(),e);
									return 0;
								}
							}
							
						});
					}
					else{
						throw new IllegalStateException(fieldname+" sorting is not supported");
					}
				}
				resMap.put(fieldname, facets);
			}
		}
		return resMap;
	}

	@Override
	protected String buildResultString(SenseiRequest req,SenseiResult res) throws Exception {
		JSONObject jsonObj = new JSONObject();
		int totalDocs = res.getTotalDocs();
		int numHits = res.getNumHits();
		jsonObj.put(PARAM_RESULT_TOTALDOCS, totalDocs);
		jsonObj.put(PARAM_RESULT_NUMHITS, numHits);
		jsonObj.put(PARAM_RESULT_PARSEDQUERY,res.getParsedQuery());
		SenseiHit[] hits = res.getSenseiHits();
		JSONArray hitArray = new JSONArray();
		jsonObj.put(PARAM_RESULT_HITS, hitArray);
		for (SenseiHit hit : hits){
			long uid = hit.getUID();
			float score = hit.getScore();
			Map<String,String[]> fieldMap = hit.getFieldValues();
			
			JSONObject hitObj = new JSONObject();
			hitObj.put(PARAM_RESULT_HIT_UID,uid);
			hitObj.put(PARAM_RESULT_HIT_SCORE, score);
			if (fieldMap!=null){
			  Set<Entry<String,String[]>> entries = fieldMap.entrySet();
			  for (Entry<String,String[]> entry : entries){
				  String key = entry.getKey();
				  String[] vals = entry.getValue();
				  
				  JSONArray valArray = new JSONArray();
				  for (String val : vals){
					  valArray.put(val);
				  }
				  hitObj.put(key, valArray);
			  }
			}
			
			Document doc = hit.getStoredFields();
			if (doc!=null){
				JSONObject storedData = new JSONObject();
				List<Fieldable> fields = doc.getFields();
				for (Fieldable field : fields){
					String name = field.name();
					String val = field.stringValue();
					storedData.put("name", name);
					storedData.put("val", val);
				}
				hitObj.put("stored", storedData);
			}
			
			Explanation expl = hit.getExplanation();
			if (expl!=null){
				hitObj.put(PARAM_RESULT_HIT_EXPLANATION, convertExpl(expl));
			}
			
			hitArray.put(hitObj);
		}
		
		jsonObj.put(PARAM_RESULT_TIME, res.getTime());
		jsonObj.put(PARAM_RESULT_FACETS, convert(res.getFacetMap(),req));
		return jsonObj.toString();
	}

	@Override
	protected SenseiRequest buildSenseiRequest(DataConfiguration params)
			throws Exception {
		int offset = params.getInt(PARAM_OFFSET, 0);
		int count = params.getInt(PARAM_COUNT,10);
		String query = params.getString(PARAM_QUERY,null);
		
		SenseiRequest senseiReq = new SenseiRequest();
		senseiReq.setOffset(offset);
		senseiReq.setCount(count);
		
		if (query!=null && query.length()>0){
			SenseiQuery sq = new StringQuery(query);
			senseiReq.setQuery(sq);
		}
		
		senseiReq.setShowExplanation(params.getBoolean(PARAM_SHOW_EXPLAIN, false));
		senseiReq.setFetchStoredFields(params.getBoolean(PARAM_FETCH_STORED,false));
		
		String[] sortStrings = params.getStringArray(PARAM_SORT);
		
		if (sortStrings!=null && sortStrings.length>0){
			ArrayList<SortField> sortFieldList = new ArrayList<SortField>(sortStrings.length);
			for (String sortString : sortStrings){
				SortField sf;
				String[] parts = sortString.split(":");
 				if (parts.length==2){
 					boolean reverse = PARAM_SORT_DESC.equals(parts[1]);
 					sf = new SortField(parts[0],SortField.CUSTOM,reverse);
 				}
 				else if (parts.length==1){
 					if (PARAM_SORT_SCORE.equals(parts[0])){
 						sf = SortField.FIELD_SCORE;
 					}
 					else{
 						sf = new SortField(parts[0],SortField.CUSTOM,false);
 					}
 				}
 				else{
 					throw new IllegalArgumentException("invalid sort string: "+sortString);
 				}
 				sortFieldList.add(sf);
			}
			
			senseiReq.setSort(sortFieldList.toArray(new SortField[sortFieldList.size()]));
		}
		

	 	Map<String,Configuration> selectParamMap = RequestConverter.parseParamConf(params, PARAM_SELECT);
	 	Set<Entry<String,Configuration>> entries = selectParamMap.entrySet();
	 	for (Entry<String,Configuration> entry : entries){
	 		String name = entry.getKey();
	 		Configuration conf = entry.getValue();
	 		
	 		BrowseSelection sel = new BrowseSelection(name);
	 		
	 		String[] vals = conf.getStringArray(PARAM_SELECT_VAL);
	 		sel.setValues(vals);
	 		
	 		vals = conf.getStringArray(PARAM_SELECT_NOT);
	 		sel.setNotValues(vals);
	 		
	 		String op = conf.getString(PARAM_SELECT_OP, PARAM_SELECT_OP_OR);
	 		
	 		ValueOperation valOp;
	 		if (PARAM_SELECT_OP_OR.equals(op)){ 
	 			valOp = ValueOperation.ValueOperationOr;
	 		}
	 		else if (PARAM_SELECT_OP_AND.equals(op)){ 

	 			valOp = ValueOperation.ValueOperationAnd;
	 		}
	 		else{
	 			throw new IllegalArgumentException("invalid selection operation: "+op);
	 		}
	 		sel.setSelectionOperation(valOp);
	 		
	 		String[] selectPropStrings = conf.getStringArray(PARAM_SELECT_PROP);
	 		if (selectPropStrings!=null && selectPropStrings.length>0){
	 			Map<String,String> prop = new HashMap<String,String>();
	 			sel.setSelectionProperties(prop);
	 			for (String selProp : selectPropStrings){
	 				String[] parts = selProp.split(":");
	 				if (parts.length==2){
	 					prop.put(parts[0], parts[1]);
	 				}
	 				else{
	 					throw new IllegalArgumentException("invalid prop string: "+selProp);
	 				}
	 			}
	 		}
	 		senseiReq.addSelection(sel);
	 		
	 	}
		
		Map<String,Configuration> facetParamMap = RequestConverter.parseParamConf(params, PARAM_FACET);
		entries = facetParamMap.entrySet();
	 	for (Entry<String,Configuration> entry : entries){
	 		String name =entry.getKey();
	 		Configuration conf = entry.getValue();
	 		FacetSpec fspec = new FacetSpec();
	 		
	 		fspec.setExpandSelection(conf.getBoolean(PARAM_FACET_EXPAND,false));
	 		fspec.setMaxCount(conf.getInt(PARAM_FACET_MAX,10));
	 		fspec.setMinHitCount(conf.getInt(PARAM_FACET_MINHIT,1));
	 		
	 		FacetSpec.FacetSortSpec orderBy;
	 		String orderString = conf.getString(PARAM_FACET_ORDER, PARAM_FACET_ORDER_HITS);
	 		if (PARAM_FACET_ORDER_HITS.equals(orderString)){
	 			orderBy = FacetSpec.FacetSortSpec.OrderHitsDesc;
	 		}
	 		else if (PARAM_FACET_ORDER_VAL.equals(orderString)){
	 			orderBy = FacetSpec.FacetSortSpec.OrderValueAsc;
	 		}
	 		else{
	 			throw new IllegalArgumentException("invalid order string: "+orderString);
	 		}
	 		fspec.setOrderBy(orderBy);
	 		senseiReq.setFacetSpec(name, fspec);
	 	}
		return senseiReq;
	}
}
