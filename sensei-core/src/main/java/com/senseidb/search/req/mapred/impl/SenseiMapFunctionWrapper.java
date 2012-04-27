package com.senseidb.search.req.mapred.impl;

import java.util.Set;

import com.linkedin.zoie.api.ZoieSegmentReader;
import com.linkedin.zoie.api.impl.DocIDMapperImpl;

import com.linkedin.bobo.api.BoboIndexReader;
import com.linkedin.bobo.mapred.BoboMapFunctionWrapper;
import com.linkedin.bobo.mapred.MapReduceResult;
import com.linkedin.bobo.util.MemoryManager;
import com.senseidb.search.req.SenseiSystemInfo.SenseiFacetInfo;
import com.senseidb.search.req.mapred.FieldAccessor;
import com.senseidb.search.req.mapred.SenseiMapReduce;
import com.senseidb.search.req.mapred.obsolete.SenseiMapReduceResult;


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
    partialDocIds = intarraymgr.get(BUFFER_SIZE);
    result = new SenseiMapReduceResult();
    this.facetInfos = facetInfos;
  }

  @Override
  public void mapFullIndexReader(BoboIndexReader reader) {
    ZoieSegmentReader<?> zoieReader = (ZoieSegmentReader<?>)(reader.getInnerReader());
    DocIDMapperImpl docIDMapper = (DocIDMapperImpl) zoieReader.getDocIDMaper();
    result.getMapResults().add(mapReduceStrategy.map(docIDMapper.getDocArray(), docIDMapper.getDocArray().length, zoieReader.getUIDArray(), new FieldAccessor(facetInfos, reader, docIDMapper)));    
  }

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
      result.getMapResults().add(mapReduceStrategy.map(docIDMapper.getDocArray(), BUFFER_SIZE, zoieReader.getUIDArray(), new FieldAccessor(facetInfos, reader, docIDMapper)));
      docIdIndex = 0;
    }
  }

  @Override
  public void finalizeSegment(BoboIndexReader reader) {
    if (docIdIndex > 0) {
      ZoieSegmentReader<?> zoieReader = (ZoieSegmentReader<?>)(reader.getInnerReader());
      DocIDMapperImpl docIDMapper = (DocIDMapperImpl) zoieReader.getDocIDMaper();
      result.getMapResults().add(mapReduceStrategy.map(partialDocIds, docIdIndex, zoieReader.getUIDArray(), new FieldAccessor(facetInfos, reader, docIDMapper)));
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
  
  protected static MemoryManager<int[]> intarraymgr = new MemoryManager<int[]>(new MemoryManager.Initializer<int[]>()
      {
        public void init(int[] buf) {         
        }
        public int[] newInstance(int size)
        {
          return new int[size];
        }
        public int size(int[] buf)
        {
          assert buf!=null;
          return buf.length;
        }
      });
}
