package com.sensei.search.nodes;

import org.apache.log4j.Logger;

import com.google.protobuf.Message;
import com.sensei.search.req.SenseiSystemInfo;
import com.sensei.search.req.protobuf.SenseiSysRequestBPO;
import com.sensei.search.req.protobuf.SenseiSysRequestBPOConverter;
import com.sensei.search.req.protobuf.SenseiSysResultBPO.SysResult;

public class SenseiNodeSysMessageHandler extends AbstractSenseiNodeMessageHandler {

	private static final Logger logger = Logger.getLogger(SenseiNodeSysMessageHandler.class);
	public SenseiNodeSysMessageHandler() {
		
	}
	
	@Override
	public Message getRequestMessage(){
	    return SenseiSysRequestBPO.SysRequest.getDefaultInstance();
	}
	  
	@Override
	public Message getResponseMessage(){
	    return SysResult.getDefaultInstance();
	}

	@Override
	public Message handleMessage(Message req) throws Exception {
		SenseiSystemInfo info = new SenseiSystemInfo(); 	// empty for now
		return SenseiSysRequestBPOConverter.convert(info);
	}
	
	
	
/*
	public Message handleMessage(Message msg) throws Exception {
		SenseiSysRequestBPO.SysRequest req = (SenseiSysRequestBPO.SysRequest) msg;
		
		if (logger.isDebugEnabled()){
			String reqString = TextFormat.printToString(req);
			reqString = reqString.replace('\r', ' ').replace('\n', ' ');
		}

		SenseiSystemInfo result = new SenseiSystemInfo();
		Set<Integer> partitions = _partReaderMap.keySet();
		if (partitions!=null && partitions.size() > 0){
			logger.info("serving partitions: "+ partitions.toString());

			Map<Integer, List<Integer>> clusterInfo = new HashMap<Integer, List<Integer>>();
			List<Integer> partitionList = new ArrayList<Integer>(partitions.size());

			Date lastModified = new Date(0L);
			DefaultZoieVersion version = (new DefaultZoieVersion.DefaultZoieVersionFactory()).getZoieVersion("0");
			for (int partition : partitions){
				partitionList.add(partition);

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
						
						if (readerList == null || readerList.size() == 0){
							logger.warn("no readers were obtained from zoie, returning no info.");
							return SenseiSysRequestBPOConverter.convert(result);
						}

						List<BoboIndexReader> boboReaders = ZoieIndexReader.extractDecoratedReaders(readerList);
						SubReaderAccessor<BoboIndexReader> subReaderAccessor = ZoieIndexReader.getSubReaderAccessor(boboReaders);

						MultiBoboBrowser browser = null;

						try {
							browser = new MultiBoboBrowser(BoboBrowser.createBrowsables(boboReaders));
							result.setNumDocs(partition, browser.numDocs());

							Set<SenseiSystemInfo.SenseiFacetInfo> facetInfos = new HashSet<SenseiSystemInfo.SenseiFacetInfo>();

							for (String name : browser.getFacetNames()) {
								facetInfos.add(new SenseiSystemInfo.SenseiFacetInfo(name));
							}
							result.setFacetInfos(facetInfos);
						} 
						catch(Exception e){
							logger.error(e.getMessage(),e);
							throw e;
						}
						finally {
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
							zoieSystem.returnIndexReaders(readerList);
						}
					}
				}
				catch(Exception e){
					logger.error(e.getMessage(),e);
				}
			}

			result.setLastModified(lastModified.getTime());
			result.setVersion(version.toString());

			clusterInfo.put(_nodeId, partitionList);
			result.setClusterInfo(clusterInfo);
		}
		else{
			logger.info("no partitions specified");
		}
		SysResult returnvalue = SenseiSysRequestBPOConverter.convert(result);
		logger.info("searching partitions	" + partitions.toString());
		return returnvalue;
	}
*/
}

