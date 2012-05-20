package com.senseidb.search.req.mapred.obsolete;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.linkedin.zoie.api.DocIDMapper;
import com.linkedin.zoie.api.ZoieSegmentReader;

import com.linkedin.bobo.api.BoboBrowser;
import com.linkedin.bobo.api.BoboIndexReader;
import com.linkedin.norbert.network.JavaSerializer;
import com.linkedin.norbert.network.Serializer;
import com.senseidb.search.node.SenseiCore;
import com.senseidb.search.node.SenseiQueryBuilderFactory;
import com.senseidb.search.req.mapred.FieldAccessor;
import com.senseidb.svc.impl.AbstractSenseiCoreService;
import com.senseidb.svc.impl.CoreSenseiServiceImpl;

public class MapReduceSenseiService extends AbstractSenseiCoreService<MapReduceRequest, SenseiMapReduceResult> {
  public static final Serializer<MapReduceRequest, SenseiMapReduceResult> SERIALIZER =
      JavaSerializer.apply("SenseiMapReduceRequest", MapReduceRequest.class, SenseiMapReduceResult.class);
  private static final Logger logger = Logger.getLogger(CoreSenseiServiceImpl.class);
 
  public MapReduceSenseiService(SenseiCore core) {
    super(core);    
  }

  @Override
  public SenseiMapReduceResult handlePartitionedRequest(MapReduceRequest request, List<BoboIndexReader> readerList, SenseiQueryBuilderFactory queryBuilderFactory) throws Exception {
    List<BoboIndexReader> segmentReaders = BoboBrowser.gatherSubReaders(readerList);
    SenseiMapReduceResult mapReduceResult = new SenseiMapReduceResult();
    mapReduceResult.setMapResults(new ArrayList<Object>(segmentReaders.size()));
    for (BoboIndexReader boboIndexReader : segmentReaders) {
      ZoieSegmentReader<?> zoieReader = (ZoieSegmentReader<?>)(boboIndexReader.getInnerReader());
      DocIDMapper<?> docidMapper = zoieReader.getDocIDMaper();     
      Object result = request.getMapReduceJob().map(zoieReader.getUIDArray(), docidMapper , new FieldAccessor(_core.getSystemInfo().getFacetInfos(), boboIndexReader, docidMapper));
      mapReduceResult.getMapResults().add(result);
    }     
    mapReduceResult.setMapResults(request.getMapReduceJob().combine(mapReduceResult.getMapResults()));   
    return mapReduceResult;
  }

  @Override
  public SenseiMapReduceResult mergePartitionedResults(MapReduceRequest request, List<SenseiMapReduceResult> resultList) {
    int size = 0;
    for (SenseiMapReduceResult reduceResult : resultList) {
      size += reduceResult.getMapResults().size();
    }
    List<Object> mapRes = new ArrayList<Object>(size);
    for (SenseiMapReduceResult reduceResult : resultList) {
      mapRes.addAll(reduceResult.getMapResults());
    }
    return (SenseiMapReduceResult) new SenseiMapReduceResult().setMapResults(request.getMapReduceJob().combine(mapRes));    
  }

  @Override
  public SenseiMapReduceResult getEmptyResultInstance(Throwable error) {    
    return new SenseiMapReduceResult();
  }

  @Override
  public Serializer<MapReduceRequest, SenseiMapReduceResult> getSerializer() {    
    return SERIALIZER;
  }


}
