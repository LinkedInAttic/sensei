package com.sensei.search.nodes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.lucene.search.Query;

import proj.zoie.api.DefaultZoieVersion;
import proj.zoie.api.IndexReaderFactory;
import proj.zoie.api.ZoieIndexReader;
import proj.zoie.api.ZoieIndexReader.SubReaderAccessor;
import proj.zoie.api.ZoieIndexReader.SubReaderInfo;
import proj.zoie.mbean.ZoieSystemAdminMBean;
import proj.zoie.impl.indexing.ZoieSystem;

import com.browseengine.bobo.api.BoboBrowser;
import com.browseengine.bobo.api.BoboIndexReader;
import com.browseengine.bobo.api.BrowseException;
import com.browseengine.bobo.api.BrowseHit;
import com.browseengine.bobo.api.BrowseRequest;
import com.browseengine.bobo.api.BrowseResult;
import com.browseengine.bobo.api.MultiBoboBrowser;
import com.google.protobuf.Message;
import com.google.protobuf.TextFormat;
import com.linkedin.norbert.javacompat.network.MessageHandler;
import com.sensei.search.client.ResultMerger;
import com.sensei.search.req.SenseiHit;
import com.sensei.search.req.SenseiSystemInfo;
import com.sensei.search.req.protobuf.SenseiSysRequestBPO;
import com.sensei.search.req.protobuf.SenseiSysRequestBPOConverter;
import com.sensei.search.req.protobuf.SenseiSysResultBPO.Result;
import com.sensei.search.util.RequestConverter;

public class SenseiNodeSysMessageHandler implements MessageHandler {

	private static final Logger logger = Logger.getLogger(SenseiNodeSysMessageHandler.class);
	private final Map<Integer,SenseiQueryBuilderFactory> _builderFactoryMap;
	private final Map<Integer,IndexReaderFactory<ZoieIndexReader<BoboIndexReader>>> _partReaderMap;

	public SenseiNodeSysMessageHandler(SenseiSearchContext ctx) {
		_builderFactoryMap = ctx.getQueryBuilderFactoryMap();
		_partReaderMap = ctx.getPartitionReaderMap();
	}
	
	public Message[] getMessages() {
		return new Message[] { SenseiSysRequestBPO.Request.getDefaultInstance() };
	}

	public Message handleMessage(Message msg) throws Exception {
		SenseiSysRequestBPO.Request req = (SenseiSysRequestBPO.Request) msg;
		
		if (logger.isDebugEnabled()){
			String reqString = TextFormat.printToString(req);
			reqString = reqString.replace('\r', ' ').replace('\n', ' ');
		}

		SenseiSystemInfo result = new SenseiSystemInfo();
		Set<Integer> partitions = _partReaderMap.keySet();
		if (partitions!=null && partitions.size() > 0){
			logger.info("serving partitions: "+ partitions.toString());
			int numDocs = 0;
			Date lastModified = new Date(0L);
			DefaultZoieVersion version = (new DefaultZoieVersion.DefaultZoieVersionFactory()).getZoieVersion("0");
			for (int partition : partitions){
				try{
					ZoieSystem<BoboIndexReader,?,DefaultZoieVersion> zoieSystem = (ZoieSystem<BoboIndexReader,?,DefaultZoieVersion>)_partReaderMap.get(partition);

					ZoieSystemAdminMBean zoieSystemAdminMBean = zoieSystem.getAdminMBean();
					if (lastModified.getTime() < zoieSystemAdminMBean.getLastDiskIndexModifiedTime().getTime())
						lastModified = zoieSystemAdminMBean.getLastDiskIndexModifiedTime();
					if (version.compareTo(zoieSystem.getVersion()) < 0)
						version = zoieSystem.getVersion();

					List<ZoieIndexReader<BoboIndexReader>> readerList = null;
					
					try{
						readerList = zoieSystem.getIndexReaders();
						if (readerList != null) {
							for (ZoieIndexReader<BoboIndexReader> reader:readerList) {
								numDocs += reader.numDocs();
							}
						}
					}
					finally{
						if (readerList!=null){
							zoieSystem.returnIndexReaders(readerList);
						}
					}
				}
				catch(Exception e){
					logger.error(e.getMessage(),e);
				}
			}
			result.setNumDocs(numDocs);
			result.setLastModified(lastModified.getTime());
			result.setVersion(version.toString());
		}
		else{
			logger.info("no partitions specified");
		}
		Result returnvalue = SenseiSysRequestBPOConverter.convert(result);
		logger.info("searching partitions	" + partitions.toString());
		return returnvalue;
	}

}

