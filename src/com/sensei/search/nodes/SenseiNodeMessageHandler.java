package com.sensei.search.nodes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import proj.zoie.api.IndexReaderFactory;
import proj.zoie.api.ZoieIndexReader;

import com.browseengine.bobo.api.BoboBrowser;
import com.browseengine.bobo.api.BoboIndexReader;
import com.browseengine.bobo.api.BrowseRequest;
import com.browseengine.bobo.api.BrowseResult;
import com.browseengine.bobo.api.MultiBoboBrowser;
import com.browseengine.bobo.facets.RuntimeFacetHandler;
import com.google.protobuf.Message;
import com.google.protobuf.TextFormat;
import com.linkedin.norbert.network.javaapi.MessageHandler;
import com.sensei.search.client.ResultMerger;
import com.sensei.search.req.FacetHandlerInitializerParam;
import com.sensei.search.req.RuntimeFacetHandlerFactory;
import com.sensei.search.req.SenseiRequest;
import com.sensei.search.req.protobuf.SenseiRequestBPO;
import com.sensei.search.req.protobuf.SenseiRequestBPOConverter;
import com.sensei.search.req.protobuf.SenseiResultBPO;
import com.sensei.search.util.RequestConverter;

public class SenseiNodeMessageHandler implements MessageHandler {

	private static final Logger logger = Logger.getLogger(SenseiNodeMessageHandler.class);
	private final SenseiQueryBuilder _qbuilder;
	private final Map<Integer,IndexReaderFactory<ZoieIndexReader<BoboIndexReader>>> _partReaderMap;
	private final Map<String,RuntimeFacetHandlerFactory> _runtimeFacetHandlerFactoryMap;

	public SenseiNodeMessageHandler(SenseiSearchContext ctx) {
		_qbuilder = ctx.getQueryBuilder();
		_partReaderMap = ctx.getPartitionReaderMap();
		List<RuntimeFacetHandlerFactory> runtimeFacetHandlerFactories = ctx
				.getRuntimeFacetHandlerFactories();
		_runtimeFacetHandlerFactoryMap = new HashMap<String,RuntimeFacetHandlerFactory>();
		if (runtimeFacetHandlerFactories!=null){
		  for (RuntimeFacetHandlerFactory factory : runtimeFacetHandlerFactories) {
			_runtimeFacetHandlerFactoryMap.put(factory.getName(), factory);
		  }
		}
	}
	
	public int[] getPartitions(){
		Set<Integer> partSet = _partReaderMap.keySet();
		int[] retSet = new int[partSet.size()];
		int c = 0;
		for (Integer part : partSet){
			retSet[c++] = part;
		}
		return retSet;
	}

	public Message[] getMessages() {
		return new Message[] { SenseiRequestBPO.Request.getDefaultInstance() };
	}
	
	private BrowseResult handleMessage(SenseiRequest senseiReq,IndexReaderFactory<ZoieIndexReader<BoboIndexReader>> readerFactory) throws Exception{
		List<ZoieIndexReader<BoboIndexReader>> readerList = null;
		
		try{
		  readerList = readerFactory.getIndexReaders();
		  
		  List<BoboIndexReader> boboReaders = ZoieIndexReader.extractDecoratedReaders(readerList);

		  MultiBoboBrowser browser = null;
          ArrayList<RuntimeFacetHandler<?>> runtimeFacetHandlers =
            new ArrayList<RuntimeFacetHandler<?>>(_runtimeFacetHandlerFactoryMap.size());

		  try {
			browser = new MultiBoboBrowser(BoboBrowser.createBrowsables(boboReaders));

			Map<String, FacetHandlerInitializerParam> initParamMaps = senseiReq
					.getAllFacetHandlerInitializerParams();
			if (initParamMaps != null) {
				Set<Entry<String, FacetHandlerInitializerParam>> entrySet = initParamMaps
						.entrySet();
				for (Entry<String, FacetHandlerInitializerParam> entry : entrySet) {
					RuntimeFacetHandlerFactory facetHandlerFactory = _runtimeFacetHandlerFactoryMap
							.get(entry.getKey());
					try {
						RuntimeFacetHandler<?> handler = facetHandlerFactory.get(entry.getValue());
						browser.setFacetHandler(handler);
						runtimeFacetHandlers.add(handler);
					} catch (Exception e1) {
						logger.error(e1.getMessage(), e1);
					}
				}
			}

			BrowseRequest breq = RequestConverter.convert(senseiReq, _qbuilder);
			BrowseResult res = browser.browse(breq);
			return res;
		  } 
		  catch(Exception e){
			logger.error(e.getMessage(),e);
			throw e;
		  }
		  finally {
		    for(RuntimeFacetHandler<?> handler : runtimeFacetHandlers){
		      try{
		        handler.close();
		      }
		      catch(Exception e) {
		        logger.error(e.getMessage(), e);
		      }
		    }
			if (browser != null) {
				try {
					browser.close();
				} catch (IOException ioe) {
					logger.error(ioe.getMessage(), ioe);
				}
			}
		  }
		}
		finally{
			if (readerList!=null){
				readerFactory.returnIndexReaders(readerList);
			}
		}
	}

	public Message handleMessage(Message msg) throws Exception {
		SenseiRequestBPO.Request req = (SenseiRequestBPO.Request) msg;
		String reqString = TextFormat.printToString(req);
		reqString = reqString.replace('\r', ' ').replace('\n', ' ');
		logger.info("received req: \n" + reqString);

		SenseiRequest senseiReq = SenseiRequestBPOConverter.convert(req);
		
		BrowseResult finalResult=null;
		int[] partitions = senseiReq.getPartitions();
		logger.info("serving partitions: "+Arrays.toString(partitions));
		if (partitions!=null && partitions.length>0){
			ArrayList<BrowseResult> resultList = new ArrayList<BrowseResult>(partitions.length);
			for (int partition : partitions){
			  try{
			    IndexReaderFactory<ZoieIndexReader<BoboIndexReader>> readerFactory=_partReaderMap.get(partition);
			    BrowseResult res = handleMessage(senseiReq, readerFactory);
			    resultList.add(res);
				finalResult = ResultMerger.merge(senseiReq, resultList);
			  }
			  catch(Exception e){
				  logger.error(e.getMessage(),e);
			  }
			}
		}
		else{
			finalResult = new BrowseResult();
		}
		SenseiResultBPO.Result resultMsg = SenseiRequestBPOConverter.convert(finalResult);
		return resultMsg;
	}

}
