/**
 * This software is licensed to you under the Apache License, Version 2.0 (the
 * "Apache License").
 *
 * LinkedIn's contributions are made under the Apache License. If you contribute
 * to the Software, the contributions will be deemed to have been made under the
 * Apache License, unless you expressly indicate otherwise. Please do not make any
 * contributions that would be inconsistent with the Apache License.
 *
 * You may obtain a copy of the Apache License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, this software
 * distributed under the Apache License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the Apache
 * License for the specific language governing permissions and limitations for the
 * software governed under the Apache License.
 *
 * © 2012 LinkedIn Corp. All Rights Reserved.  
 */
package com.senseidb.search.req.mapred.impl;

import java.util.ArrayList;
import java.util.Set;

import proj.zoie.api.ZoieSegmentReader;
import proj.zoie.api.impl.DocIDMapperImpl;

import com.browseengine.bobo.api.BoboIndexReader;
import com.browseengine.bobo.facets.FacetCountCollector;
import com.browseengine.bobo.mapred.BoboMapFunctionWrapper;
import com.browseengine.bobo.mapred.MapReduceResult;
import com.browseengine.bobo.util.MemoryManager;
import com.senseidb.search.req.SenseiSystemInfo.SenseiFacetInfo;
import com.senseidb.search.req.mapred.CombinerStage;
import com.senseidb.search.req.mapred.FacetCountAccessor;
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
  public void mapFullIndexReader(BoboIndexReader reader, FacetCountCollector[] facetCountCollectors) {
    ZoieSegmentReader<?> zoieReader = (ZoieSegmentReader<?>)(reader.getInnerReader());
    DocIDMapperImpl docIDMapper = (DocIDMapperImpl) zoieReader.getDocIDMaper();
    result.getMapResults().add(mapReduceStrategy.map(docIDMapper.getDocArray(), docIDMapper.getDocArray().length, zoieReader.getUIDArray(), new FieldAccessor(facetInfos, reader, docIDMapper), new FacetCountAccessor(facetCountCollectors)));    
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
      result.getMapResults().add(mapReduceStrategy.map(partialDocIds, BUFFER_SIZE, zoieReader.getUIDArray(), new FieldAccessor(facetInfos, reader, docIDMapper),  FacetCountAccessor.EMPTY));
      docIdIndex = 0;
    }
  }

  /* (non-Javadoc)
   * @see com.browseengine.bobo.mapred.BoboMapFunctionWrapper#finalizeSegment(com.browseengine.bobo.api.BoboIndexReader)
   */
  @Override
  public void finalizeSegment(BoboIndexReader reader, FacetCountCollector[] facetCountCollectors) {
    
    if (docIdIndex > 0) {
      ZoieSegmentReader<?> zoieReader = (ZoieSegmentReader<?>)(reader.getInnerReader());
      DocIDMapperImpl docIDMapper = (DocIDMapperImpl) zoieReader.getDocIDMaper();
      result.getMapResults().add(mapReduceStrategy.map(partialDocIds, docIdIndex, zoieReader.getUIDArray(), new FieldAccessor(facetInfos, reader, docIDMapper), new FacetCountAccessor(facetCountCollectors)));    
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
