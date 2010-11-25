package com.sensei.search.client.servlet;

import static com.sensei.search.client.servlet.SenseiSearchServletParams.PARAM_COUNT;
import static com.sensei.search.client.servlet.SenseiSearchServletParams.PARAM_FACET;
import static com.sensei.search.client.servlet.SenseiSearchServletParams.PARAM_FETCH_STORED;
import static com.sensei.search.client.servlet.SenseiSearchServletParams.PARAM_OFFSET;
import static com.sensei.search.client.servlet.SenseiSearchServletParams.PARAM_QUERY;
import static com.sensei.search.client.servlet.SenseiSearchServletParams.PARAM_RESULT_FACETS;
import static com.sensei.search.client.servlet.SenseiSearchServletParams.PARAM_RESULT_HITS;
import static com.sensei.search.client.servlet.SenseiSearchServletParams.PARAM_RESULT_HIT_EXPLANATION;
import static com.sensei.search.client.servlet.SenseiSearchServletParams.PARAM_RESULT_HIT_SCORE;
import static com.sensei.search.client.servlet.SenseiSearchServletParams.PARAM_RESULT_HIT_UID;
import static com.sensei.search.client.servlet.SenseiSearchServletParams.PARAM_RESULT_NUMHITS;
import static com.sensei.search.client.servlet.SenseiSearchServletParams.PARAM_RESULT_TIME;
import static com.sensei.search.client.servlet.SenseiSearchServletParams.PARAM_RESULT_TOTALDOCS;
import static com.sensei.search.client.servlet.SenseiSearchServletParams.PARAM_SELECT;
import static com.sensei.search.client.servlet.SenseiSearchServletParams.PARAM_SHOW_EXPLAIN;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
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
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.browseengine.bobo.api.BrowseFacet;
import com.browseengine.bobo.api.BrowseSelection;
import com.browseengine.bobo.api.FacetAccessible;
import com.browseengine.bobo.api.FacetSpec;
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
		

	 	Map<String,Configuration> selectParamMap = RequestConverter.parseParamConf(params, PARAM_SELECT);
		
		Map<String,Configuration> facetParamMap = RequestConverter.parseParamConf(params, PARAM_FACET);
		return senseiReq;
	}
	
	public static void main(String[] args) {
		String s = "a.b.c.d";
		String s2 = "a";
		String s4 = "";
		
		System.out.println(Arrays.toString(s4.split("\\.")));
	}

}
