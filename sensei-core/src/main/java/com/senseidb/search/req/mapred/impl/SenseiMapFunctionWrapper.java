package com.senseidb.search.req.mapred.impl;

import java.util.ArrayList;
import java.util.Set;

import proj.zoie.api.ZoieSegmentReader;
import proj.zoie.api.impl.DocIDMapperImpl;

import com.browseengine.bobo.api.BoboIndexReader;
import com.browseengine.bobo.mapred.BoboMapFunctionWrapper;
import com.browseengine.bobo.mapred.MapReduceResult;
import com.browseengine.bobo.util.MemoryManager;
import com.senseidb.search.req.SenseiSystemInfo.SenseiFacetInfo;
import com.senseidb.search.req.mapred.CombinerStage;
import com.senseidb.search.req.mapred.FieldAccessor;
import com.senseidb.search.req.mapred.SenseiMapReduce;


/**
 * Inctance of this class is the part of the senseiReuqest, and it keep the intermediate step of the map reduce job
 * @author vzhabiuk
 *
 */
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
    result = new MapReduceResult();
    this.facetInfos = facetInfos;
  }

  /* (non-Javadoc)
   * @see com.browseengine.bobo.mapred.BoboMapFunctionWrapper#mapFullIndexReader(com.browseengine.bobo.api.BoboIndexReader)
   */
  @Override
  public void mapFullIndexReader(BoboIndexReader reader) {
    ZoieSegmentReader<?> zoieReader = (ZoieSegmentReader<?>)(reader.getInnerReader());
    DocIDMapperImpl docIDMapper = (DocIDMapperImpl) zoieReader.getDocIDMaper();
    result.getMapResults().add(mapReduceStrategy.map(docIDMapper.getDocArray(), docIDMapper.getDocArray().length, zoieReader.getUIDArray(), new FieldAccessor(facetInfos, reader, docIDMapper)));    
  }

  /* (non-Javadoc)
   * @see com.browseengine.bobo.mapred.BoboMapFunctionWrapper#mapSingleDocument(int, com.browseengine.bobo.api.BoboIndexReader)
   */
  @Override
  public final void mapSingleDocument(int docId, BoboIndexReader reader) {
    if (docIdIndex < BUFFER_SIZE - 1) {
      partialDocIds[docIdIndex++] = docId;
      return;
    }
    if (docIdIndex == BUFFER_SIZE - 1) {
      partialDocIds[docIdIndex++] = docId;
      ZoieSegmentReader<?> zoieReader = (ZoieSegmentReader<?>)(reader.getInnerReader());
      DocIDMapperImpl docIDMapper = (DocIDMapperImpl) zoieReader.getDocIDMaper();
      result.getMapResults().add(mapReduceStrategy.map(partialDocIds, BUFFER_SIZE, zoieReader.getUIDArray(), new FieldAccessor(facetInfos, reader, docIDMapper)));
      docIdIndex = 0;
    }
  }

  /* (non-Javadoc)
   * @see com.browseengine.bobo.mapred.BoboMapFunctionWrapper#finalizeSegment(com.browseengine.bobo.api.BoboIndexReader)
   */
  @Override
  public void finalizeSegment(BoboIndexReader reader) {
    
    if (docIdIndex > 0) {
      ZoieSegmentReader<?> zoieReader = (ZoieSegmentReader<?>)(reader.getInnerReader());
      DocIDMapperImpl docIDMapper = (DocIDMapperImpl) zoieReader.getDocIDMaper();
      result.getMapResults().add(mapReduceStrategy.map(partialDocIds, docIdIndex, zoieReader.getUIDArray(), new FieldAccessor(facetInfos, reader, docIDMapper)));    
    }
    docIdIndex = 0;
  }

  /* (non-Javadoc)
   * @see com.browseengine.bobo.mapred.BoboMapFunctionWrapper#finalizePartition()
   */
  @Override
  public void finalizePartition() {
    result.setMapResults(new ArrayList(mapReduceStrategy.combine(result.getMapResults(), CombinerStage.partitionLevel))) ;    
  }

  @Override
  public MapReduceResult getResult() {
    return result;
  }
  
}
