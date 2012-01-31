package com.senseidb.search.req.mapred.impl;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import proj.zoie.api.DocIDMapper;
import proj.zoie.api.ZoieSegmentReader;

import com.browseengine.bobo.api.BoboBrowser;
import com.browseengine.bobo.api.BoboIndexReader;
import com.linkedin.norbert.network.JavaSerializer;
import com.linkedin.norbert.network.Serializer;
import com.senseidb.search.node.SenseiCore;
import com.senseidb.search.node.SenseiQueryBuilderFactory;
import com.senseidb.svc.impl.AbstractSenseiCoreService;
import com.senseidb.svc.impl.CoreSenseiServiceImpl;

public class MapReduceSenseiService extends AbstractSenseiCoreService<MapReduceRequest, MapReduceResult> {
  public static final Serializer<MapReduceRequest, MapReduceResult> SERIALIZER =
      JavaSerializer.apply("SenseiMapReduceRequest", MapReduceRequest.class, MapReduceResult.class);
  private static final Logger logger = Logger.getLogger(CoreSenseiServiceImpl.class);
 
  public MapReduceSenseiService(SenseiCore core) {
    super(core);    
  }

  @Override
  public MapReduceResult handlePartitionedRequest(MapReduceRequest request, List<BoboIndexReader> readerList, SenseiQueryBuilderFactory queryBuilderFactory) throws Exception {
    List<BoboIndexReader> segmentReaders = BoboBrowser.gatherSubReaders(readerList);
    MapReduceResult mapReduceResult = new MapReduceResult();
    mapReduceResult.setMapResults(new ArrayList<Object>(segmentReaders.size()));
    for (BoboIndexReader boboIndexReader : segmentReaders) {
      ZoieSegmentReader<?> zoieReader = (ZoieSegmentReader<?>)(boboIndexReader.getInnerReader());
      DocIDMapper<?> docidMapper = zoieReader.getDocIDMaper();     
      Object result = request.getMapReduceJob().map(zoieReader.getUIDArray(), docidMapper , new FieldAccessorImpl(_core.getSystemInfo().getFacetInfos(), boboIndexReader, docidMapper));
      mapReduceResult.getMapResults().add(result);
    }     
    mapReduceResult.setMapResults(request.getMapReduceJob().combine(mapReduceResult.getMapResults()));   
    return mapReduceResult;
  }

  @Override
  public MapReduceResult mergePartitionedResults(MapReduceRequest request, List<MapReduceResult> resultList) {
    int size = 0;
    for (MapReduceResult reduceResult : resultList) {
      size += reduceResult.getMapResults().size();
    }
    List<Object> mapRes = new ArrayList<Object>(size);
    for (MapReduceResult reduceResult : resultList) {
      mapRes.addAll(reduceResult.getMapResults());
    }
    return new MapReduceResult().setMapResults(request.getMapReduceJob().combine(mapRes));    
  }

  @Override
  public MapReduceResult getEmptyResultInstance(Throwable error) {    
    return new MapReduceResult();
  }

  @Override
  public Serializer<MapReduceRequest, MapReduceResult> getSerializer() {    
    return SERIALIZER;
  }


}
