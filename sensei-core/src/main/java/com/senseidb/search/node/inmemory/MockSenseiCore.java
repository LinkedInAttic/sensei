package com.senseidb.search.node.inmemory;

import java.util.List;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.util.Version;

import proj.zoie.api.IndexReaderFactory;
import proj.zoie.api.ZoieIndexReader;

import com.browseengine.bobo.api.BoboIndexReader;
import com.senseidb.indexing.SenseiIndexPruner;
import com.senseidb.search.node.SenseiCore;
import com.senseidb.search.node.impl.DefaultJsonQueryBuilderFactory;

public class MockSenseiCore extends SenseiCore {

  private final MockIndexReaderFactory<ZoieIndexReader<BoboIndexReader>> mockIndexReaderFactory;

  public MockSenseiCore(MockIndexReaderFactory<ZoieIndexReader<BoboIndexReader>> mockIndexReaderFactory) {
    super(0, new int[] { 0 }, null, null, new DefaultJsonQueryBuilderFactory(new QueryParser(Version.LUCENE_35, "contents", new StandardAnalyzer(Version.LUCENE_35))));
    this.mockIndexReaderFactory = mockIndexReaderFactory;
    setIndexPruner(new SenseiIndexPruner.DefaultSenseiIndexPruner());
  }

  @Override
  public IndexReaderFactory<ZoieIndexReader<BoboIndexReader>> getIndexReaderFactory(int partition) {

    return mockIndexReaderFactory;
  }

}
