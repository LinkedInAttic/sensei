package com.sensei.search.svc.impl;

import com.browseengine.bobo.api.BoboIndexReader;
import com.linkedin.norbert.network.Serializer;
import com.sensei.search.nodes.SenseiCore;
import com.sensei.search.nodes.SenseiQueryBuilderFactory;
import com.sensei.search.req.AbstractSenseiRequest;
import com.sensei.search.req.AbstractSenseiResult;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import org.apache.log4j.Logger;
import proj.zoie.api.IndexReaderFactory;
import proj.zoie.api.ZoieIndexReader;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public abstract class AbstractSenseiCoreService<Req extends AbstractSenseiRequest, Res extends AbstractSenseiResult>{
    private final static Logger logger = Logger.getLogger(AbstractSenseiCoreService.class);
    
	protected final SenseiCore _core;
	
	public AbstractSenseiCoreService(SenseiCore core){
	  _core = core;
	}

  public final Res execute(Req senseiReq) {
    Set<Integer> partitions = senseiReq.getPartitions();

    if(partitions == null)
      partitions = new IntOpenHashSet(_core.getPartitions());

    ArrayList<Res> resultList = new ArrayList<Res>(partitions.size());
    for (int partition : partitions) {
      try {
        long start = System.currentTimeMillis();
        IndexReaderFactory<ZoieIndexReader<BoboIndexReader>> readerFactory = _core.getIndexReaderFactory(partition);
        Res res = handleRequest(senseiReq, readerFactory, _core.getQueryBuilderFactory(partition));
        resultList.add(res);
        long end = System.currentTimeMillis();
        res.setTime(end - start);
        logger.info("searching partition: " + partition + " browse took: " + res.getTime());
      } catch (Exception e) {
        logger.error(e.getMessage(), e);
        resultList.add(getEmptyResultInstance(e));
      }
    }

    Res finalResult = mergePartitionedResults(senseiReq, resultList);
    if (logger.isInfoEnabled()) {
      logger.info("searching partitions took: " + finalResult.getTime());
    }
    return finalResult;
  }

  private final Res handleRequest(Req senseiReq, IndexReaderFactory<ZoieIndexReader<BoboIndexReader>> readerFactory, SenseiQueryBuilderFactory queryBuilderFactory) throws Exception {
    List<ZoieIndexReader<BoboIndexReader>> readerList = null;
    try {
      readerList = readerFactory.getIndexReaders();
      if (logger.isDebugEnabled()) {
        logger.debug("obtained readerList of size: " + readerList == null ? 0 : readerList.size());
      }
      List<BoboIndexReader> boboReaders = ZoieIndexReader.extractDecoratedReaders(readerList);
      return handlePartitionedRequest(senseiReq, boboReaders, queryBuilderFactory);
    } finally {
      if (readerFactory != readerList) {
        readerFactory.returnIndexReaders(readerList);
      }
    }
  }

  public abstract Res handlePartitionedRequest(Req r,final List<BoboIndexReader> readerList,SenseiQueryBuilderFactory queryBuilderFactory) throws Exception;
	public abstract Res mergePartitionedResults(Req r,List<Res> reqList);
	public abstract Res getEmptyResultInstance(Throwable error);

  public abstract Serializer<Req, Res> getSerializer();
}
