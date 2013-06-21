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
package com.senseidb.search.node.inmemory;


import java.util.Collections;

import java.util.List;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.util.Version;

import proj.zoie.api.IndexReaderFactory;
import proj.zoie.api.ZoieIndexReader;

import com.browseengine.bobo.api.BoboIndexReader;
import com.senseidb.indexing.SenseiIndexPruner;
import com.senseidb.search.node.SenseiCore;
import com.senseidb.search.node.SenseiIndexReaderDecorator;
import com.senseidb.search.node.impl.DefaultJsonQueryBuilderFactory;

public class MockSenseiCore extends SenseiCore {


  private final ThreadLocal<MockIndexReaderFactory<ZoieIndexReader<BoboIndexReader>>> mockIndexReaderFactory = new ThreadLocal<MockIndexReaderFactory<ZoieIndexReader<BoboIndexReader>>>();
  private final int[] partitions;
  private static MockIndexReaderFactory<ZoieIndexReader<BoboIndexReader>> emptyIndexFactory = new MockIndexReaderFactory<ZoieIndexReader<BoboIndexReader>>(Collections.EMPTY_LIST);
  public MockSenseiCore(int[] partitions, SenseiIndexReaderDecorator senseiIndexReaderDecorator) {
    super(0, new int[] { 0 }, null, null, new DefaultJsonQueryBuilderFactory(new QueryParser(Version.LUCENE_35, "contents",
        new StandardAnalyzer(Version.LUCENE_35))), senseiIndexReaderDecorator);
    this.partitions = partitions;
    setIndexPruner(new SenseiIndexPruner.DefaultSenseiIndexPruner());
  }

  @Override
  public IndexReaderFactory<ZoieIndexReader<BoboIndexReader>> getIndexReaderFactory(int partition) {

    if (partition == partitions[0])
    return mockIndexReaderFactory.get();
    else {
      return emptyIndexFactory;
    }
  }
  public void setIndexReaderFactory(IndexReaderFactory<ZoieIndexReader<BoboIndexReader>> indexReaderFactory) {
    mockIndexReaderFactory.set((MockIndexReaderFactory<ZoieIndexReader<BoboIndexReader>>)indexReaderFactory);
  }
  @Override
  public int[] getPartitions() {
    return partitions;
  }
}
