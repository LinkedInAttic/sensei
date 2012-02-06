package com.senseidb.search.req.mapred.v2.impl;

import java.util.Set;

import proj.zoie.api.ZoieSegmentReader;
import proj.zoie.api.impl.DocIDMapperImpl;

import com.browseengine.bobo.api.BoboIndexReader;
import com.browseengine.bobo.mapred.BoboMapFunctionWrapper;
import com.browseengine.bobo.mapred.MapReduceResult;
import com.senseidb.search.req.SenseiSystemInfo.SenseiFacetInfo;
import com.senseidb.search.req.mapred.impl.FieldAccessorImpl;
import com.senseidb.search.req.mapred.impl.SenseiMapReduceResult;
import com.senseidb.search.req.mapred.v2.SenseiMapReduce;


public class SenseiMapFunctionWrapper implements BoboMapFunctionWrapper {
  private MapReduceResult result;
  private SenseiMapReduce mapReduceStrategy;
  private Set<SenseiFacetInfo> facetInfos;
  public static final int BUFFER_SIZE = 2048;
  private int[] partialDocIds;;
  private int docIdIndex = 0;
  public SenseiMapFunctionWrapper(SenseiMapReduce mapReduceStrategy, Set<SenseiFacetInfo> facetInfos) {
    super();
    this.mapReduceStrategy = mapReduceStrategy;   
    partialDocIds = new int[BUFFER_SIZE];
    result = new SenseiMapReduceResult();
    
  }

  @Override
  public void mapFullIndexReader(BoboIndexReader reader) {
    ZoieSegmentReader<?> zoieReader = (ZoieSegmentReader<?>)(reader.getInnerReader());
    DocIDMapperImpl docIDMapper = (DocIDMapperImpl) zoieReader.getDocIDMaper();
    result.getMapResults().add(mapReduceStrategy.map(docIDMapper.getDocArray(), docIDMapper.getDocArray().length, zoieReader.getUIDArray(), new FieldAccessorImpl(facetInfos, reader, docIDMapper)));    
  }

  @Override
  public void mapSingleDocument(int docId, BoboIndexReader reader) {
    if (docIdIndex < BUFFER_SIZE - 1) {
      partialDocIds[docIdIndex++] = docId;
      return;
    }
    if (docIdIndex == BUFFER_SIZE - 1) {
      partialDocIds[docIdIndex++] = docId;
      ZoieSegmentReader<?> zoieReader = (ZoieSegmentReader<?>)(reader.getInnerReader());
      DocIDMapperImpl docIDMapper = (DocIDMapperImpl) zoieReader.getDocIDMaper();
      result.getMapResults().add(mapReduceStrategy.map(docIDMapper.getDocArray(), BUFFER_SIZE, zoieReader.getUIDArray(), new FieldAccessorImpl(facetInfos, reader, docIDMapper)));
    }
  }

  @Override
  public void finalizeSegment(BoboIndexReader reader) {
    if (docIdIndex > 0) {
      ZoieSegmentReader<?> zoieReader = (ZoieSegmentReader<?>)(reader.getInnerReader());
      DocIDMapperImpl docIDMapper = (DocIDMapperImpl) zoieReader.getDocIDMaper();
      result.getMapResults().add(mapReduceStrategy.map(docIDMapper.getDocArray(), BUFFER_SIZE, zoieReader.getUIDArray(), new FieldAccessorImpl(facetInfos, reader, docIDMapper)));
    }
    docIdIndex = 0;
  }

  @Override
  public void finalizePartition() {
    result.setMapResults(mapReduceStrategy.combine(result.getMapResults()));    
  }

  @Override
  public MapReduceResult getResult() {
    return result;
  }
}
