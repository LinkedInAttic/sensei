package com.sensei.search.req.protobuf;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.log4j.Logger;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.SortField;

import com.browseengine.bobo.api.BrowseFacet;
import com.browseengine.bobo.api.BrowseSelection;
import com.browseengine.bobo.api.FacetAccessible;
import com.browseengine.bobo.api.FacetIterator;
import com.browseengine.bobo.api.FacetSpec;
import com.browseengine.bobo.api.BrowseSelection.ValueOperation;
import com.browseengine.bobo.api.FacetSpec.FacetSortSpec;
import com.browseengine.bobo.facets.DefaultFacetHandlerInitializerParam;
import com.browseengine.bobo.facets.FacetHandlerInitializerParam;
import com.google.protobuf.ByteString;
import com.google.protobuf.TextFormat;
import com.google.protobuf.TextFormat.ParseException;
import com.sensei.search.req.SenseiHit;
import com.sensei.search.req.SenseiQuery;
import com.sensei.search.req.SenseiRequest;
import com.sensei.search.req.SenseiResult;
import com.sensei.search.req.protobuf.SenseiRequestBPO.ByteArrayParam;
import com.sensei.search.req.protobuf.SenseiRequestBPO.StringParam;

public class SenseiRequestBPOConverter {

	private static Logger logger = Logger.getLogger(SenseiRequestBPOConverter.class);
	
	/**
	 * Converts a list of protobuf FacetHandlerInitializerParam to Bobo's FacetHandlerInitializerParam.
	 * @param paramList
	 * @return
	 * @throws ParseException
	 */
	public static Map<String,FacetHandlerInitializerParam> convert(List<SenseiRequestBPO.FacetHandlerInitializerParam> paramList) throws ParseException {

		Map<String,FacetHandlerInitializerParam> retMap = new HashMap<String,FacetHandlerInitializerParam>();
		for (SenseiRequestBPO.FacetHandlerInitializerParam param : paramList){
			String name = param.getName();
			DefaultFacetHandlerInitializerParam initParam = new DefaultFacetHandlerInitializerParam();
			retMap.put(name, initParam);
			
			// boolean values
			List<SenseiRequestBPO.ByteArrayParam> boolparamList = param.getBoolParamsList();
			for (SenseiRequestBPO.ByteArrayParam boolparam : boolparamList){
			  String paramName = boolparam.getName();
			  boolean[] value = ProtoConvertUtil.toBooleanArray(boolparam.getVal());
			  initParam.putBooleanParam(paramName,value);
			}
			
			// int values
			List<SenseiRequestBPO.ByteArrayParam> intparamList = param.getIntParamsList();
			for (SenseiRequestBPO.ByteArrayParam intparam : intparamList){
			  String paramName = intparam.getName();
			  int[] value = ProtoConvertUtil.toIntArray(intparam.getVal());
			  initParam.putIntParam(paramName,value);
			}
			
			// long values
			List<SenseiRequestBPO.ByteArrayParam> longparamList = param.getLongParamsList();
			for (SenseiRequestBPO.ByteArrayParam longparam : longparamList){
			  String paramName = longparam.getName();
			  long[] value = ProtoConvertUtil.toLongArray(longparam.getVal());
			  initParam.putLongParam(paramName,value);
			}
			
			// String values
			List<SenseiRequestBPO.StringParam> strparamList = param.getStringParamsList();
			for (SenseiRequestBPO.StringParam strparam : strparamList){
			  String paramName = strparam.getName();
			  List<String> value = strparam.getValsList();
			  initParam.putStringParam(paramName,value);
			}
			
			// double values
			List<SenseiRequestBPO.ByteArrayParam> doubleparamList = param.getDoubleParamsList();
			for (SenseiRequestBPO.ByteArrayParam doubleparam : doubleparamList){
			  String paramName = doubleparam.getName();
			  double[] value = ProtoConvertUtil.toDoubleArray(doubleparam.getVal());
			  initParam.putDoubleParam(paramName,value);
			}
			
			// byte[] values
			List<SenseiRequestBPO.ByteArrayParam> byteparamList = param.getBytesParmasList();
			for (SenseiRequestBPO.ByteArrayParam byteparam : byteparamList){
			  String paramName = byteparam.getName();
			  ByteString value = byteparam.getVal();
			  initParam.putByteArrayParam(paramName,value.toByteArray());
			}	
		}
		return retMap;
	}
	
	/**
   * Converts a list of protobuf FacetHandlerInitializerParam to Bobo's FacetHandlerInitializerParam.
	 * @param paramMap
	 * @return
	 * @throws ParseException
	 */
	public static List<SenseiRequestBPO.FacetHandlerInitializerParam> convert(Map<String,FacetHandlerInitializerParam> paramMap) throws ParseException{
		List<SenseiRequestBPO.FacetHandlerInitializerParam> paramList = new ArrayList<SenseiRequestBPO.FacetHandlerInitializerParam>(paramMap.size());
		Set<Entry<String,FacetHandlerInitializerParam>> entrySet = paramMap.entrySet();
		for (Entry<String,FacetHandlerInitializerParam> entry : entrySet){
			
		  String name = entry.getKey();
		  FacetHandlerInitializerParam param = entry.getValue();
		  
		  if (param==null) continue;
		  
		  SenseiRequestBPO.FacetHandlerInitializerParam.Builder subBuilder = SenseiRequestBPO.FacetHandlerInitializerParam.newBuilder();
		  subBuilder.setName(name);
		  
		  // boolean
		  Set<String> paramNames = param.getBooleanParamNames();
		  List<ByteArrayParam> boolparamList = new ArrayList<ByteArrayParam>(paramNames.size());
		  for (String paramName : paramNames){
			ByteArrayParam.Builder paramBuilder = ByteArrayParam.newBuilder();
			paramBuilder.setName(paramName);
			paramBuilder.setVal(ProtoConvertUtil.serializeData(param.getBooleanParam(paramName)));
			boolparamList.add(paramBuilder.build());
		  }
		  subBuilder.addAllBoolParams(boolparamList);
		  
		  // int
		  paramNames = param.getIntParamNames();
		  List<ByteArrayParam> intparamList = new ArrayList<ByteArrayParam>(paramNames.size());
		  for (String paramName : paramNames){
			ByteArrayParam.Builder paramBuilder = ByteArrayParam.newBuilder();
			paramBuilder.setName(paramName);
			paramBuilder.setVal(ProtoConvertUtil.serializeData(param.getIntParam(paramName)));
			intparamList.add(paramBuilder.build());
		  }
		  subBuilder.addAllIntParams(intparamList);
		  
		// long
		  paramNames = param.getLongParamNames();
		  List<ByteArrayParam> longparamList = new ArrayList<ByteArrayParam>(paramNames.size());
		  for (String paramName : paramNames){
			ByteArrayParam.Builder paramBuilder = ByteArrayParam.newBuilder();
			paramBuilder.setName(paramName);
			paramBuilder.setVal(ProtoConvertUtil.serializeData(param.getLongParam(paramName)));
			longparamList.add(paramBuilder.build());
		  }
		  subBuilder.addAllLongParams(longparamList);
		  
		// double
		  paramNames = param.getDoubleParamNames();
		  List<ByteArrayParam> doubleparamList = new ArrayList<ByteArrayParam>(paramNames.size());
		  for (String paramName : paramNames){
			ByteArrayParam.Builder paramBuilder = ByteArrayParam.newBuilder();
			paramBuilder.setName(paramName);
			paramBuilder.setVal(ProtoConvertUtil.serializeData(param.getDoubleParam(paramName)));
			doubleparamList.add(paramBuilder.build());
		  }
		  subBuilder.addAllDoubleParams(doubleparamList);
		  
		// string
		  paramNames = param.getStringParamNames();
		  List<StringParam> stringparamList = new ArrayList<StringParam>(paramNames.size());
		  for (String paramName : paramNames){
			StringParam.Builder paramBuilder = StringParam.newBuilder();
			paramBuilder.setName(paramName);
			paramBuilder.addAllVals(param.getStringParam(paramName));
			stringparamList.add(paramBuilder.build());
		  }
		  subBuilder.addAllStringParams(stringparamList);
		  
		  //bytes
		  paramNames = param.getByteArrayParamNames();
		  List<ByteArrayParam> bytearrayparamList = new ArrayList<ByteArrayParam>(paramNames.size());
		  for (String paramName : paramNames){
			ByteArrayParam.Builder paramBuilder = ByteArrayParam.newBuilder();
			paramBuilder.setName(paramName);
			paramBuilder.setVal(ByteString.copyFrom(param.getByteArrayParam(paramName)));
			bytearrayparamList.add(paramBuilder.build());
		  }
		  subBuilder.addAllBytesParmas(bytearrayparamList);
		  
		  paramList.add(subBuilder.build());
		}
		return paramList;
	}
	
	private static class FacetContainerAccessible implements FacetAccessible{
		private Map<String,BrowseFacet> _data;
		private List<BrowseFacet> _datalist;
		FacetContainerAccessible(SenseiResultBPO.FacetContainer facetContainer){
			_data = new HashMap<String,BrowseFacet>();
			if (facetContainer!=null){
				List<SenseiResultBPO.Facet> facetList = facetContainer.getFacetsList();
	      _datalist = new ArrayList<BrowseFacet>(facetList.size());
				if (facetList!=null){
					for (SenseiResultBPO.Facet facet : facetList){
						BrowseFacet bfacet = new BrowseFacet();
						String val = facet.getVal();
						bfacet.setValue(val);
						bfacet.setFacetValueHitCount(facet.getCount());
						_data.put(val,bfacet);
						_datalist.add(bfacet);
					}
				}
			}
		}
		public BrowseFacet getFacet(String value) {
			return _data.get(value);
		}

		public List<BrowseFacet> getFacets() {
			return _datalist;
		}
		
		@Override
		public void close() {
			
		}
		
		@Override
		public FacetIterator iterator() {
			throw new IllegalStateException("FacetIterator should not be obtained by FacetContainer");
		}
	}
	
	private static com.sensei.search.req.protobuf.SenseiResultBPO.Explanation convert(Explanation explain){
		if (explain!=null){
          com.sensei.search.req.protobuf.SenseiResultBPO.Explanation.Builder explBlder = com.sensei.search.req.protobuf.SenseiResultBPO.Explanation.newBuilder();
          explBlder.setDescription(explain.getDescription());
          explBlder.setValue(explain.getValue());
          Explanation[] subExpls = explain.getDetails();
          if (subExpls!=null && subExpls.length>0){
        	  for (Explanation subExpl : subExpls){
        		  com.sensei.search.req.protobuf.SenseiResultBPO.Explanation sub = convert(subExpl);
        		  if (sub!=null){
        		    explBlder.addDetails(sub);
        		  }
        	  }
          }
          return explBlder.build();
        }
		else{
			return null;
		}
	}
	

	private static  Explanation convert(com.sensei.search.req.protobuf.SenseiResultBPO.Explanation explain){
		if (explain!=null){
		    Explanation expl = new Explanation();
		    expl.setDescription(explain.getDescription());
		    expl.setValue(explain.getValue());
		    List<com.sensei.search.req.protobuf.SenseiResultBPO.Explanation> detailList = explain.getDetailsList();
		    if (detailList!=null){
		    	for (com.sensei.search.req.protobuf.SenseiResultBPO.Explanation detail :detailList){
		    		Explanation subExpl = convert(detail);
		    		if (subExpl!=null){
		    			expl.addDetail(subExpl);
		    		}
		    	}
		    }
		    return expl;
		}
		else{
			return null;
		}
	}
	
	public static SenseiHit convert(SenseiResultBPO.Hit hit){
		SenseiHit bhit = new SenseiHit();
        bhit.setUID(hit.getUid());
        bhit.setDocid(hit.getDocid());
		bhit.setScore(hit.getScore());
		List<SenseiResultBPO.FieldVal> fieldValueList = hit.getFieldValuesList();
		Map<String,String[]> fielddata = new HashMap<String,String[]>();
		for (SenseiResultBPO.FieldVal fieldVal : fieldValueList){
			List<String> valList = fieldVal.getValsList();
			fielddata.put(fieldVal.getName(), valList.toArray(new String[valList.size()]));
		}
		bhit.setFieldValues(fielddata);
		Document document = new Document();
		for ( SenseiResultBPO.StoredField storedField : hit.getStoredFieldsList() ) {
			for ( String value : storedField.getValsList() ) {
				document.add( new Field( storedField.getName(), value, Field.Store.YES, Field.Index.NOT_ANALYZED ) );
			}
		}
		bhit.setStoredFields( document );
		bhit.setExplanation(convert(hit.getExplanation()));
		return bhit;
	}
	
	public static SenseiResult convert(SenseiResultBPO.Result result){
		long time = result.getTime();
		int numhits = result.getNumhits();
		int totaldocs = result.getTotaldocs();
		List<SenseiResultBPO.FacetContainer> facetList = result.getFacetContainersList();
		List<SenseiResultBPO.Hit> hitList = result.getHitsList();
		SenseiResult res = new SenseiResult();
    // set transaction ID to trace transactions
		res.setTid(result.getTid());
		res.setTime(time);
		res.setTotalDocs(totaldocs);
		res.setNumHits(numhits);
		for (SenseiResultBPO.FacetContainer facetContainer : facetList)
		{
			res.addFacets(facetContainer.getName(), new FacetContainerAccessible(facetContainer));
		}
		
		SenseiHit[] senseiHits = new SenseiHit[hitList==null ? 0 : hitList.size()];
		int i=0;
		for (SenseiResultBPO.Hit hit : hitList)
		{
			senseiHits[i++] = convert(hit);
		}
		res.setHits(senseiHits);
		return res;
	}
	
	public static SenseiRequest convert(SenseiRequestBPO.Request req) throws ParseException{
		SenseiRequest breq = new SenseiRequest();
		// set transaction ID to trace transactions
		breq.setTid(req.getTid());
		ByteString byteString = req.getQuery();
		if (byteString!=null){
		  breq.setQuery(new SenseiQuery(byteString.toByteArray()));	
		}
		breq.setOffset(req.getOffset());
		breq.setCount(req.getCount());
		breq.setShowExplanation(req.getShowExplanation());
		
		ByteString filterBytes = req.getFilterUids();
		
		if (filterBytes!=null){
			long[] filterIds = ProtoConvertUtil.toLongArray(filterBytes);
			boolean filterOutIds = req.getFilterOutIds();
			breq.setFilterUids(filterIds, filterOutIds);
		}
		
		
		breq.setFetchStoredFields(req.getFetchStoredFields());
		breq.setPartitions(ProtoConvertUtil.toIntegerSet(req.getPartitions()));
		// FacetHandlerInitializerParameters
		List<SenseiRequestBPO.FacetHandlerInitializerParam> paramList = req.getFacetInitParamsList();
		Map<String,FacetHandlerInitializerParam> params = convert(paramList);
		breq.putAllFacetHandlerInitializerParams(params);
		int i = 0;
		
		List<SenseiRequestBPO.Sort> sortList = req.getSortList();
		SortField[] sortFields = new SortField[sortList == null ? 0 : sortList.size()];
		for (SenseiRequestBPO.Sort s : sortList){
			String fieldname = s.getField();
			if (fieldname!=null && fieldname.length() == 0){
				fieldname=null;
			}
			SortField sf = new SortField(fieldname,s.getType(),s.getReverse());
			sortFields[i++] = sf;
		}
		if (sortFields.length > 0){
		 breq.setSort(sortFields);
		}
		// Facet Specs
		List<SenseiRequestBPO.FacetSpec> fspecList = req.getFacetSpecsList();
		for (SenseiRequestBPO.FacetSpec fspec : fspecList){
			FacetSpec facetSpec = new FacetSpec();
			facetSpec.setExpandSelection(fspec.getExpand());
			facetSpec.setMaxCount(fspec.getMax());
			facetSpec.setMinHitCount(fspec.getMinCount());
			SenseiRequestBPO.FacetSpec.SortSpec fsort = fspec.getOrderBy();
			if (fsort == SenseiRequestBPO.FacetSpec.SortSpec.HitsDesc)
			{
				facetSpec.setOrderBy(FacetSortSpec.OrderHitsDesc);
			}
			else
			{
				facetSpec.setOrderBy(FacetSortSpec.OrderValueAsc);	
			}
			breq.setFacetSpec(fspec.getName(), facetSpec);
		}
		
		List<SenseiRequestBPO.Selection> selList = req.getSelectionsList();
		for (SenseiRequestBPO.Selection sel : selList){
			BrowseSelection bsel = null;
			
			List<String> vals = sel.getValuesList();
			if (vals!=null)
			{
				if (bsel==null)
				{
					bsel = new BrowseSelection(sel.getName());
				}
				bsel.setValues(vals.toArray(new String[vals.size()]));
				
			}
			vals = sel.getNotValuesList();
			if (vals!=null)
			{
				if (bsel==null)
				{
					bsel = new BrowseSelection(sel.getName());
				}
				bsel.setNotValues(vals.toArray(new String[vals.size()]));
				
			}
			
			if (bsel!= null){
				SenseiRequestBPO.Selection.Operation operation = sel.getOp();
				if (operation == SenseiRequestBPO.Selection.Operation.OR){
					bsel.setSelectionOperation(ValueOperation.ValueOperationOr);
				}
				else{
					bsel.setSelectionOperation(ValueOperation.ValueOperationAnd);
				}
				List<SenseiRequestBPO.Property> props = sel.getPropsList();
				if (props!=null)
				{
				  for (SenseiRequestBPO.Property prop : props){
					  bsel.setSelectionProperty(prop.getKey(), prop.getVal());
				  }
				}
				breq.addSelection(bsel);
			}
			
		}
		return breq;
	}
	
	public static SenseiRequestBPO.Selection convert(BrowseSelection sel){
		String name = sel.getFieldName();
		String[] vals = sel.getValues();
		String[] notVals = sel.getNotValues();
		ValueOperation op =sel.getSelectionOperation();
		Properties props = sel.getSelectionProperties();
		
		SenseiRequestBPO.Selection.Builder selBuilder = SenseiRequestBPO.Selection.newBuilder();
		selBuilder.setName(name);
		selBuilder.addAllValues(Arrays.asList(vals));
		selBuilder.addAllNotValues(Arrays.asList(notVals));
		if (op == ValueOperation.ValueOperationAnd){
		  selBuilder.setOp(SenseiRequestBPO.Selection.Operation.AND);
		}
		else{
		  selBuilder.setOp(SenseiRequestBPO.Selection.Operation.OR);
		}
		Iterator iter = props.keySet().iterator();
		while(iter.hasNext()){
			String key = (String)iter.next();
			String val = props.getProperty(key);
			SenseiRequestBPO.Property prop = SenseiRequestBPO.Property.newBuilder().setKey(key).setVal(val).build();
			selBuilder.addProps(prop);
		}
		return selBuilder.build();
	}
	
	public static SenseiRequestBPO.Request convert(SenseiRequest req) throws ParseException{
		SenseiRequestBPO.Request.Builder reqBuilder = SenseiRequestBPO.Request.newBuilder();
    // set transaction ID to trace transactions
		reqBuilder.setTid(req.getTid());
		reqBuilder.setOffset(req.getOffset());
		reqBuilder.setCount(req.getCount());
		SenseiQuery q = req.getQuery();
		
		// filterIds
		long[] filterIds = req.getFilterUids();
		boolean isFilterOut = req.isFilterOutIds();
		
		reqBuilder.setFilterOutIds(isFilterOut);
		if (filterIds!=null){
			ByteString idBytes = ProtoConvertUtil.serializeData(filterIds);
			reqBuilder.setFilterUids(idBytes);
		}
		

		ByteString queryBytes = null;
		if (q!=null){
			queryBytes = ByteString.copyFrom(q.toBytes());
			reqBuilder.setQuery(queryBytes);
		}
		
		Set<Integer> partitionSet = req.getPartitions();
		int[] partitions = null;
		if(partitionSet != null) {
		  partitions = new int[partitionSet.size()];
		  int index = 0;
		  for(int partition : partitionSet) {
		    partitions[index++] = partition;
		  }
		}
		ByteString partitionBytes=ProtoConvertUtil.serializeData(partitions); 
		if (partitionBytes!=null){
	 	  reqBuilder.setPartitions(partitionBytes);
		}

		reqBuilder.setFetchStoredFields(req.isFetchStoredFields());
		Map<String,FacetHandlerInitializerParam> initParamMap = req.getAllFacetHandlerInitializerParams();
		if (initParamMap!=null){
		  reqBuilder.addAllFacetInitParams(convert(initParamMap));
		}
		
		// selections
		BrowseSelection[] selections = req.getSelections();
		for (BrowseSelection sel : selections){
			reqBuilder.addSelections(convert(sel));
		}
		
		// sort
		SortField[] sortfields = req.getSort();
		for (SortField sortfield : sortfields){
			String fn = sortfield.getField();
			SenseiRequestBPO.Sort.Builder sortBuilder = SenseiRequestBPO.Sort.newBuilder();
			if (fn!=null){
				sortBuilder.setField(fn);
			}
			SenseiRequestBPO.Sort sort = sortBuilder.setReverse(sortfield.getReverse()).setType(sortfield.getType()).build();
			reqBuilder.addSort(sort);
		}
		
		// facetspec
		Map<String,FacetSpec> facetSpecMap = req.getFacetSpecs();
		Iterator<Entry<String,FacetSpec>> iter = facetSpecMap.entrySet().iterator();
		while(iter.hasNext()){
			Entry<String,FacetSpec> entry = iter.next();
			FacetSpec fspec = entry.getValue();
			if (fspec!=null)
			{
				SenseiRequestBPO.FacetSpec.Builder facetspecBuilder = SenseiRequestBPO.FacetSpec.newBuilder();
				facetspecBuilder.setName(entry.getKey());
				facetspecBuilder.setExpand(fspec.isExpandSelection());
				facetspecBuilder.setMax(fspec.getMaxCount());
				facetspecBuilder.setMinCount(fspec.getMinHitCount());
				if (fspec.getOrderBy() == FacetSortSpec.OrderHitsDesc){
				  facetspecBuilder.setOrderBy(SenseiRequestBPO.FacetSpec.SortSpec.HitsDesc);
				}
				else{
				  facetspecBuilder.setOrderBy(SenseiRequestBPO.FacetSpec.SortSpec.ValueAsc);
				}
				reqBuilder.addFacetSpecs(facetspecBuilder);
			}
			else{
				logger.warn("facet handler: "+entry.getKey()+" is null, skipped");
			}
		}
		return reqBuilder.build();
	}
	
	public static SenseiResultBPO.Hit convert(SenseiHit hit){
		SenseiResultBPO.Hit.Builder hitBuilder = SenseiResultBPO.Hit.newBuilder();
        hitBuilder.setDocid(hit.getDocid());
		hitBuilder.setScore(hit.getScore());
		Map<String,String[]> fieldMap = hit.getFieldValues();
		Iterator<Entry<String,String[]>> iter = fieldMap.entrySet().iterator();
		while(iter.hasNext()){
			Entry<String,String[]> entry = iter.next();
			String name = entry.getKey();
			String[] vals = entry.getValue();
			if (vals!=null){
			  SenseiResultBPO.FieldVal fieldVal = SenseiResultBPO.FieldVal.newBuilder().setName(name).addAllVals(Arrays.asList(vals)).build();
			  hitBuilder.addFieldValues(fieldVal);
			}
			else{
			  logger.error("null values for: "+name+", not added");
			}
		}
		if ( null != hit.getStoredFields() ) {
			for( Object fieldObj : hit.getStoredFields().getFields() ) {
				Field field = (Field)fieldObj;
				SenseiResultBPO.StoredField storedField = (
					SenseiResultBPO.StoredField.newBuilder().setName(
						field.name()
					)
				).addAllVals( Arrays.asList( field.stringValue() ) ).build();
				hitBuilder.addStoredFields( storedField );
			}
		}
        hitBuilder.setUid(hit.getUID());
        
        // explanation
        Explanation explain = hit.getExplanation();
        com.sensei.search.req.protobuf.SenseiResultBPO.Explanation exp = convert(explain);
        if (exp!=null){
          hitBuilder.setExplanation(exp);
        }
		return hitBuilder.build();
	}
	
	public static SenseiResultBPO.FacetContainer convert(String name,FacetAccessible facetAccessible){
		SenseiResultBPO.FacetContainer.Builder facetBuilder = SenseiResultBPO.FacetContainer.newBuilder();
		facetBuilder.setName(name);
		List<BrowseFacet> list = facetAccessible.getFacets();
		for (BrowseFacet facet : list){
			SenseiResultBPO.Facet f = SenseiResultBPO.Facet.newBuilder().setVal(facet.getValue()).setCount(facet.getFacetValueHitCount()).build();
			facetBuilder.addFacets(f);
		}
		return facetBuilder.build();
	}
	
	public static SenseiResultBPO.Result convert(SenseiResult res){
		SenseiResultBPO.Result.Builder resBuilder = SenseiResultBPO.Result.newBuilder();
    // set transaction ID to trace transactions
		resBuilder.setTid(res.getTid());
		resBuilder.setTime(res.getTime());
		resBuilder.setTotaldocs(res.getTotalDocs());
		resBuilder.setNumhits(res.getNumHits());
		
		// hits
		SenseiHit[] hits = res.getSenseiHits();
		for (SenseiHit hit : hits){
			SenseiResultBPO.Hit converted = convert(hit);
			resBuilder.addHits(converted);
		}
		
		// facet containers
		Map<String,FacetAccessible> facetMap = res.getFacetMap();
		Iterator<Entry<String,FacetAccessible>> iter = facetMap.entrySet().iterator();
		while(iter.hasNext()){
			Entry<String,FacetAccessible> entry = iter.next();
			SenseiResultBPO.FacetContainer converted = convert(entry.getKey(),entry.getValue());
			resBuilder.addFacetContainers(converted);
		}
		return resBuilder.build();
	}
	
	public static String toProtoBufString(SenseiRequest req) throws ParseException{
		SenseiRequestBPO.Request protoReq = convert(req);
		String outString = TextFormat.printToString(protoReq);
		outString = outString.replace('\r', ' ').replace('\n', ' ');
		return outString;
	}
	
	public static SenseiRequest fromProtoBufString(String str) throws ParseException{
		SenseiRequestBPO.Request.Builder protoReqBuilder = SenseiRequestBPO.Request.newBuilder();
		TextFormat.merge(str, protoReqBuilder);
		SenseiRequestBPO.Request protoReq = protoReqBuilder.build();
		try{
		return convert(protoReq);
		}
		catch(Exception e){
			throw new ParseException(e.getMessage());
		}
	}
}
